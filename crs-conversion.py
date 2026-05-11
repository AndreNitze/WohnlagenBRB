import argparse
from pathlib import Path

import pandas as pd
from pyproj import Transformer


SOURCE_CRS = "EPSG:25833"
TARGET_CRS = "EPSG:4326"
X_COL = "x"
Y_COL = "y"
EXPECTED_X_RANGE = (300000, 400000)
EXPECTED_Y_RANGE = (5700000, 5900000)

DATASETS = {
    "haltestellen": {
        "input": Path("data/2026_Haltestellen.csv"),
        "output": Path("out/haltestellen_geocoded.csv"),
        "renames": {
            "name": "Name_Haltestelle",
            "kategorie": "Kategorie",
            "Tagbusse": "Stadtlinien",
            "Anzahl der Stadtlinien (mit Nachtbus)": "Anzahl der Linien",
        },
    },
    "einzelhandel": {
        "input": Path("data/2026_Einzelhandel.csv"),
        "output": Path("out/einzelhandel_geocoded.csv"),
        "renames": {
            "Unternehmen": "Name_Unternehmen",
        },
    },
}


def parse_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )


def make_merge_addr(name: object) -> str:
    value = str(name).strip().lower()
    if not value or value == "nan":
        return "brandenburg an der havel"
    if "haltestelle" not in value:
        value = f"haltestelle {value}"
    return " ".join(value.split())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transformiert x/y-Koordinaten aus EPSG:25833 nach WGS84.",
    )
    parser.add_argument(
        "dataset",
        choices=sorted(DATASETS),
        help="Datensatz, der transformiert werden soll.",
    )
    return parser.parse_args()


def normalize_coordinates(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    df[X_COL] = parse_number(df[X_COL])
    df[Y_COL] = parse_number(df[Y_COL])

    invalid_coords = df[X_COL].isna() | df[Y_COL].isna()
    if invalid_coords.any():
        raise ValueError(f"Ungueltige Koordinaten in {int(invalid_coords.sum())} Zeilen.")

    x_ok = df[X_COL].between(*EXPECTED_X_RANGE)
    y_ok = df[Y_COL].between(*EXPECTED_Y_RANGE)
    swapped = (
        ~x_ok
        & ~y_ok
        & df[X_COL].between(*EXPECTED_Y_RANGE)
        & df[Y_COL].between(*EXPECTED_X_RANGE)
    )
    swap_count = int(swapped.sum())
    if swap_count:
        df.loc[swapped, [X_COL, Y_COL]] = df.loc[swapped, [Y_COL, X_COL]].to_numpy()

    invalid_range = ~df[X_COL].between(*EXPECTED_X_RANGE) | ~df[Y_COL].between(*EXPECTED_Y_RANGE)
    if invalid_range.any():
        bad_rows = df.loc[invalid_range, [X_COL, Y_COL]].head()
        raise ValueError(f"Koordinaten ausserhalb des erwarteten Bereichs:\n{bad_rows}")

    return df, swap_count


def convert_dataset(dataset: str) -> None:
    config = DATASETS[dataset]
    input_csv = config["input"]
    output_csv = config["output"]

    df = pd.read_csv(input_csv, sep=",", encoding="utf-8-sig")
    df = df.rename(
        columns={
            source: target
            for source, target in config["renames"].items()
            if source in df.columns and target not in df.columns
        }
    )

    missing_columns = [col for col in [X_COL, Y_COL] if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Pflichtspalten fehlen: {missing_columns}")

    df, swap_count = normalize_coordinates(df)
    if swap_count:
        print(f"Hinweis: {swap_count} Zeilen mit vertauschten x/y-Koordinaten korrigiert.")

    if dataset == "haltestellen" and "Name_Haltestelle" in df.columns:
        df["Name_Haltestelle"] = df["Name_Haltestelle"].astype(str).str.strip()
        df["Adresse_merge"] = df["Name_Haltestelle"].apply(make_merge_addr)
    elif dataset == "einzelhandel":
        if "Name_Unternehmen" in df.columns:
            df["Name_Unternehmen"] = df["Name_Unternehmen"].astype(str).str.strip()
        if "Straßenname" in df.columns and "Hsnr" in df.columns:
            hsnr = df["Hsnr"].astype(str).str.strip()
            if "HsnrZus" in df.columns:
                hsnr_zus = df["HsnrZus"].astype(str).str.strip()
            else:
                hsnr_zus = pd.Series("", index=df.index)
            hsnr_zus = hsnr_zus.mask(hsnr_zus.str.lower().isin(["", "nan"]), "")
            df["Adresse_merge"] = (
                df["Straßenname"].astype(str).str.strip().str.lower()
                + " "
                + hsnr.str.lower()
                + hsnr_zus.str.lower()
            ).str.replace(r"\s+", " ", regex=True).str.strip()

    transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)
    lon, lat = transformer.transform(df[X_COL].to_numpy(), df[Y_COL].to_numpy())

    df["lon"] = lon
    df["lat"] = lat
    df["geometry"] = [f"POINT ({x} {y})" for x, y in zip(df["lon"], df["lat"])]

    duplicate_columns = [col for col in ["Name_Haltestelle", "Kategorie", X_COL, Y_COL] if col in df.columns]
    if len(duplicate_columns) == 4:
        duplicate_count = int(df.duplicated(duplicate_columns).sum())
        if duplicate_count:
            print(f"Hinweis: {duplicate_count} doppelte Haltestellenzeilen nach Name/Kategorie/x/y.")

    if "Anzahl der Linien" in df.columns:
        missing_lines = int(pd.to_numeric(df["Anzahl der Linien"], errors="coerce").isna().sum())
        if missing_lines:
            print(f"Hinweis: {missing_lines} Zeilen ohne 'Anzahl der Linien'.")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, sep=",", index=False, encoding="utf-8")

    print(f"Geschrieben: {output_csv}")
    print(f"Zeilen: {len(df)}")
    print(f"lat: {df['lat'].min():.6f} bis {df['lat'].max():.6f}")
    print(f"lon: {df['lon'].min():.6f} bis {df['lon'].max():.6f}")
    print(df.head())


def main() -> None:
    args = parse_args()
    convert_dataset(args.dataset)


if __name__ == "__main__":
    main()
