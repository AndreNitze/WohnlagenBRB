import argparse

import numpy as np
import pandas as pd


LINE_COLUMN = "Anzahl der Linien"
TARGET_PREFIX = "haltestellen"
COORD_DECIMALS = 7


def normalize_name(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.casefold()


def coord_key(lat: pd.Series, lon: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(lat, errors="coerce").round(COORD_DECIMALS).astype("string")
        + "|"
        + pd.to_numeric(lon, errors="coerce").round(COORD_DECIMALS).astype("string")
    )


def to_numeric_lines(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def unique_mapping(df: pd.DataFrame, key_col: str, value_col: str) -> pd.Series:
    relevant = df[[key_col, value_col]].dropna(subset=[key_col, value_col]).copy()
    relevant = relevant[relevant[key_col] != ""]

    unique_values = relevant.groupby(key_col)[value_col].nunique(dropna=True)
    unique_keys = unique_values[unique_values == 1].index

    return (
        relevant[relevant[key_col].isin(unique_keys)]
        .drop_duplicates(key_col)
        .set_index(key_col)[value_col]
    )


def fill_missing(target: pd.Series, replacement: pd.Series) -> tuple[pd.Series, int]:
    missing_before = target.isna().sum()
    target = target.where(target.notna(), replacement)
    return target, int(missing_before - target.isna().sum())


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ergänzt in adressen_mit_haltestellen_routen.csv die Spalte "
            f"'{LINE_COLUMN}' aus haltestellen_geocoded.csv."
        )
    )
    parser.add_argument(
        "--addresses",
        default="out/adressen_mit_haltestellen_routen.csv",
        help="Pfad zur Adress-CSV",
    )
    parser.add_argument(
        "--stops",
        default="out/haltestellen_geocoded.csv",
        help="Pfad zur Haltestellen-CSV",
    )
    parser.add_argument(
        "--output",
        default="out/adressen_mit_haltestellen_routen.csv",
        help="Pfad zur Ausgabe-CSV (Default: überschreibt addresses)",
    )
    args = parser.parse_args()

    df_addr = pd.read_csv(args.addresses, encoding="utf-8")
    df_stops = pd.read_csv(args.stops, encoding="utf-8")

    if LINE_COLUMN not in df_stops.columns:
        raise ValueError(f"Spalte '{LINE_COLUMN}' fehlt in der Haltestellen-CSV.")

    df_stops[LINE_COLUMN] = to_numeric_lines(df_stops[LINE_COLUMN])
    line_counts = pd.Series(np.nan, index=df_addr.index, dtype="float64")
    match_counts: dict[str, int] = {}

    target_id_col = f"{TARGET_PREFIX}_target_id"
    if target_id_col in df_addr.columns:
        target_ids = pd.to_numeric(df_addr[target_id_col], errors="coerce")
        valid_ids = target_ids.notna() & target_ids.astype("Int64").isin(df_stops.index)
        by_id = pd.Series(np.nan, index=df_addr.index, dtype="float64")
        by_id.loc[valid_ids] = df_stops.loc[
            target_ids.loc[valid_ids].astype(int),
            LINE_COLUMN,
        ].to_numpy()
        line_counts, match_counts["target_id"] = fill_missing(line_counts, by_id)

    target_lat_col = f"{TARGET_PREFIX}_target_lat"
    target_lon_col = f"{TARGET_PREFIX}_target_lon"
    target_name_col = f"{TARGET_PREFIX}_target_name"

    if {target_lat_col, target_lon_col, target_name_col}.issubset(df_addr.columns) and {
        "lat",
        "lon",
        "Name_Haltestelle",
    }.issubset(df_stops.columns):
        stop_keys = pd.DataFrame(
            {
                "__name_coord_key": normalize_name(df_stops["Name_Haltestelle"])
                + "|"
                + coord_key(df_stops["lat"], df_stops["lon"]),
                LINE_COLUMN: df_stops[LINE_COLUMN],
            }
        )
        addr_key = normalize_name(df_addr[target_name_col]) + "|" + coord_key(
            df_addr[target_lat_col], df_addr[target_lon_col]
        )
        mapping = unique_mapping(stop_keys, "__name_coord_key", LINE_COLUMN)
        line_counts, match_counts["name_coord"] = fill_missing(line_counts, addr_key.map(mapping))

    if {target_lat_col, target_lon_col}.issubset(df_addr.columns) and {"lat", "lon"}.issubset(
        df_stops.columns
    ):
        stop_keys = pd.DataFrame(
            {
                "__coord_key": coord_key(df_stops["lat"], df_stops["lon"]),
                LINE_COLUMN: df_stops[LINE_COLUMN],
            }
        )
        mapping = unique_mapping(stop_keys, "__coord_key", LINE_COLUMN)
        addr_key = coord_key(df_addr[target_lat_col], df_addr[target_lon_col])
        line_counts, match_counts["coord"] = fill_missing(line_counts, addr_key.map(mapping))

    if target_name_col in df_addr.columns and "Name_Haltestelle" in df_stops.columns:
        stop_keys = pd.DataFrame(
            {
                "__name_key": normalize_name(df_stops["Name_Haltestelle"]),
                LINE_COLUMN: df_stops[LINE_COLUMN],
            }
        )
        mapping = unique_mapping(stop_keys, "__name_key", LINE_COLUMN)
        line_counts, match_counts["name"] = fill_missing(
            line_counts, normalize_name(df_addr[target_name_col]).map(mapping)
        )

    # Das Notebook benennt "Anzahl der Linien" nach dem Laden in
    # "haltestellen_linien_count" um. Eine bereits vorhandene gleichnamige
    # Spalte wuerde nach rename() zu doppelten Spalten und Pandas-ValueErrors
    # fuehren.
    df_addr = df_addr.drop(columns=["haltestellen_linien_count"], errors="ignore")
    df_addr[LINE_COLUMN] = line_counts.astype("Int64")
    df_addr.to_csv(args.output, index=False, encoding="utf-8")

    missing = df_addr[LINE_COLUMN].isna().sum()
    print(f"Geschrieben: {args.output}")
    print(f"Zeilen: {len(df_addr)} | fehlende {LINE_COLUMN}: {missing}")
    print(
        "Matches: "
        + ", ".join(f"{match_type}={count}" for match_type, count in match_counts.items())
    )


if __name__ == "__main__":
    main()
