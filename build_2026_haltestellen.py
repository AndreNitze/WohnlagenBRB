import shutil
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

BASE_2024_CSV = Path("data/2024_Haltestellen.csv")
CORRECTION_XLSX = Path("data/haltestellen_korrektur.xlsx")
OUTPUT_2026_CSV = Path("data/2026_Haltestellen.csv")


def normalize_text(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    text = str(value)
    if text.strip().lower() in {"nan", "none", "nat"}:
        return ""
    text = text.replace("\u00ad", "")  # soft hyphen
    text = text.replace("\u00a0", " ")
    text = " ".join(text.strip().split())
    return text


def normalize_key(value):
    return normalize_text(value).lower()


def normalize_category(value):
    cat = normalize_key(value).replace(" ", "")
    if cat in {"tram_stop,bus_stop", "bus_stop,tram_stop", "tram_stop_bus_stop", "bus_stop_tram_stop"}:
        return "tram_stop_bus_stop"
    return cat


def load_correction_dataframe(path: Path) -> pd.DataFrame:
    csv_neighbor = path.with_suffix(".csv")
    if csv_neighbor.exists():
        return pd.read_csv(csv_neighbor)

    try:
        return pd.read_excel(path)
    except Exception:
        if shutil.which("libreoffice") is None:
            raise

        with tempfile.TemporaryDirectory(prefix="haltestellen_korrektur_") as tmpdir:
            result = subprocess.run(
                [
                    "libreoffice",
                    "--headless",
                    "--convert-to",
                    "csv",
                    "--outdir",
                    tmpdir,
                    str(path),
                ],
                check=False,
            )
            csv_path = Path(tmpdir) / f"{path.stem}.csv"
            if not csv_path.exists() and result.returncode != 0:
                raise RuntimeError(
                    f"LibreOffice-Konvertierung fehlgeschlagen (Exit {result.returncode}) und keine CSV erzeugt."
                )
            if not csv_path.exists():
                raise FileNotFoundError(f"Konvertierte CSV nicht gefunden: {csv_path}")
            return pd.read_csv(csv_path)


def coalesce(df: pd.DataFrame, primary_cols, fallback_cols):
    target = pd.Series([None] * len(df), index=df.index, dtype="object")
    for col in list(primary_cols) + list(fallback_cols):
        if col not in df.columns:
            continue
        values = df[col].map(normalize_text)
        mask = target.isna() | (target.map(normalize_text) == "")
        fill_mask = mask & values.ne("")
        target.loc[fill_mask] = values.loc[fill_mask]
    return target


def build_2026_haltestellen(base_csv: Path, correction_xlsx: Path, output_csv: Path) -> dict:
    base = pd.read_csv(base_csv, dtype=str).copy()
    if not {"Name_Haltestelle", "Kategorie", "x", "y"}.issubset(base.columns):
        raise KeyError("2024_Haltestellen.csv hat nicht die erwarteten Spalten.")

    corr_raw = load_correction_dataframe(correction_xlsx)
    corr = corr_raw.rename(
        columns={
            "name": "Name_Haltestelle",
            "kategorie": "Kategorie",
            "Unnamed: 9": "Stadtlinien",
            "Anzahl der Stadtlinien (mit Nachtbus)": "Anzahl der Linien",
        }
    ).copy()

    if "Anzahl der Linien" not in corr.columns and "Anzahl der Stadtlinien (ohne Nachtbus)" in corr.columns:
        corr["Anzahl der Linien"] = corr["Anzahl der Stadtlinien (ohne Nachtbus)"]

    corr["Name_Haltestelle"] = corr.get("Name_Haltestelle", "").map(normalize_text)
    corr["Kategorie"] = corr.get("Kategorie", "").map(normalize_text)
    corr["x"] = corr.get("x", "").map(normalize_text)
    corr["y"] = corr.get("y", "").map(normalize_text)

    corr["name_norm"] = corr["Name_Haltestelle"].map(normalize_key)
    corr["cat_norm"] = corr["Kategorie"].map(normalize_category)

    if "ist im Routing" in corr.columns:
        corr["ist im Routing"] = pd.to_numeric(corr["ist im Routing"], errors="coerce").fillna(0).astype(int)
    else:
        corr["ist im Routing"] = 0

    base["Name_Haltestelle"] = base["Name_Haltestelle"].map(normalize_text)
    base["Kategorie"] = base["Kategorie"].map(normalize_text)
    base["x"] = base["x"].map(normalize_text)
    base["y"] = base["y"].map(normalize_text)

    base["name_norm"] = base["Name_Haltestelle"].map(normalize_key)
    base["cat_norm"] = base["Kategorie"].map(normalize_category)

    # Für Metadaten bevorzugt Routing=1, dann hohe Linienanzahl.
    corr["linien_int"] = pd.to_numeric(corr.get("Anzahl der Linien"), errors="coerce")
    corr_meta = corr.sort_values(["ist im Routing", "linien_int"], ascending=[False, False]).copy()

    meta_cols = [
        "gid",
        "Punkt liegt auf Haltestelle",
        "in BRB a.d. Havel",
        "nicht gegenüberliegende Haltestelle",
        "Stadtlinien",
        "Nachtbusse",
        "Regiobusse",
        "Regionalzüge",
        "Anzahl der Linien",
    ]
    for col in meta_cols:
        if col not in corr_meta.columns:
            corr_meta[col] = None

    meta_pair = corr_meta.drop_duplicates(["name_norm", "cat_norm"], keep="first")
    meta_name = corr_meta.drop_duplicates(["name_norm"], keep="first")

    base = base.merge(
        meta_pair[["name_norm", "cat_norm"] + meta_cols],
        on=["name_norm", "cat_norm"],
        how="left",
        suffixes=("", "_pair"),
    )

    base = base.merge(
        meta_name[["name_norm"] + meta_cols],
        on=["name_norm"],
        how="left",
        suffixes=("", "_name"),
    )

    for col in meta_cols:
        base[col] = coalesce(base, [col], [f"{col}_pair", f"{col}_name"])

    # Fehlende, markierte Haltestellen: Koordinaten von gleichnamigen Gegenhaltestellen übernehmen.
    donors = corr[corr["ist im Routing"] == 1].copy()
    donors = donors[(donors["x"] != "") & (donors["y"] != "")]
    donor_pair = donors.drop_duplicates(["name_norm", "cat_norm"], keep="first")[["name_norm", "cat_norm", "x", "y"]]
    donor_name = donors.drop_duplicates(["name_norm"], keep="first")[["name_norm", "x", "y"]]

    missing_marked = corr[corr["ist im Routing"] == 0][["name_norm", "cat_norm"]].drop_duplicates().copy()
    missing_marked = missing_marked.merge(
        donor_pair.rename(columns={"x": "x_pair", "y": "y_pair"}),
        on=["name_norm", "cat_norm"],
        how="left",
    )
    missing_marked = missing_marked.merge(
        donor_name.rename(columns={"x": "x_name", "y": "y_name"}),
        on=["name_norm"],
        how="left",
    )
    missing_marked["x_donor"] = coalesce(missing_marked, ["x_pair"], ["x_name"])
    missing_marked["y_donor"] = coalesce(missing_marked, ["y_pair"], ["y_name"])

    base = base.merge(
        missing_marked[["name_norm", "cat_norm", "x_donor", "y_donor"]],
        on=["name_norm", "cat_norm"],
        how="left",
    )

    donor_mask = base["x_donor"].map(normalize_text).ne("") & base["y_donor"].map(normalize_text).ne("")
    base.loc[donor_mask, "x"] = base.loc[donor_mask, "x_donor"]
    base.loc[donor_mask, "y"] = base.loc[donor_mask, "y_donor"]

    # Fallback für gid: aus id ableiten, wenn keine gid-Metadaten existieren.
    base["gid"] = coalesce(base, ["gid"], ["id"])

    output_cols = [
        "Name_Haltestelle",
        "Kategorie",
        "gid",
        "x",
        "y",
        "Punkt liegt auf Haltestelle",
        "in BRB a.d. Havel",
        "nicht gegenüberliegende Haltestelle",
        "Stadtlinien",
        "Nachtbusse",
        "Regiobusse",
        "Regionalzüge",
        "Anzahl der Linien",
    ]

    # Ergänze Korrektur-Einträge, die nicht in 2024 enthalten sind (abweichende Benennung/Kategorie).
    base_keys = set(zip(base["name_norm"], base["cat_norm"]))
    corr_union = corr_meta.drop_duplicates(["name_norm", "cat_norm"], keep="first").copy()
    corr_union = corr_union.loc[
        ~corr_union.apply(lambda row: (row["name_norm"], row["cat_norm"]) in base_keys, axis=1)
    ].copy()

    if not corr_union.empty:
        corr_union = corr_union.merge(
            missing_marked[["name_norm", "cat_norm", "x_donor", "y_donor"]],
            on=["name_norm", "cat_norm"],
            how="left",
        )
        corr_union_donor_mask = corr_union["x_donor"].map(normalize_text).ne("") & corr_union["y_donor"].map(normalize_text).ne("")
        corr_union.loc[corr_union_donor_mask, "x"] = corr_union.loc[corr_union_donor_mask, "x_donor"]
        corr_union.loc[corr_union_donor_mask, "y"] = corr_union.loc[corr_union_donor_mask, "y_donor"]

        for col in output_cols:
            if col not in corr_union.columns:
                corr_union[col] = ""

        corr_union["Name_Haltestelle"] = corr_union["Name_Haltestelle"].map(normalize_text)
        corr_union["Kategorie"] = corr_union["Kategorie"].map(normalize_text)
        corr_union["x"] = corr_union["x"].map(normalize_text)
        corr_union["y"] = corr_union["y"].map(normalize_text)
        corr_union["gid"] = corr_union["gid"].map(normalize_text)

    output = pd.concat(
        [
            base[output_cols].copy(),
            corr_union[output_cols].copy() if not corr_union.empty else pd.DataFrame(columns=output_cols),
        ],
        ignore_index=True,
    )

    # Aufräumen: konsistente leere Strings statt NaN.
    output = output.fillna("")
    output["Anzahl der Linien"] = pd.to_numeric(output["Anzahl der Linien"], errors="coerce").fillna(0).astype(int)

    output.to_csv(output_csv, index=False, encoding="utf-8")

    stats = {
        "base_rows": int(len(base)),
        "extra_rows_from_correction": int(len(corr_union)),
        "output_rows": int(len(output)),
        "marked_missing_keys": int(len(missing_marked)),
        "donor_rows_applied": int(donor_mask.sum()),
        "distinct_stops": int(output[["Name_Haltestelle", "Kategorie"]].drop_duplicates().shape[0]),
    }
    return stats


if __name__ == "__main__":
    stats = build_2026_haltestellen(BASE_2024_CSV, CORRECTION_XLSX, OUTPUT_2026_CSV)
    print("Neue 2026_Haltestellen.csv erstellt:")
    for key, value in stats.items():
        print(f"- {key}: {value}")
