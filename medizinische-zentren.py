"""
Medizinische Versorgung – Pipeline
---------------------------------
Ziel (laut Anforderung):
"Fußläufige Entfernung zu Apotheken, wobei zusätzlich mind. 2 Arztpraxen im Umkreis von 100 m erreichbar sein müssen."

Aktueller Scope dieses Skripts (Schritt A = Zentrenbildung):
1) Ärztedaten (Praxen) und Apothekendaten laden. Erwartet wird EPSG:25833 (x/y in Metern).
2) Für jede Apotheke wird im 100‑m-Radius gezählt, wie viele *distinct* Praxen vorhanden sind.
   - Distinct-Schlüssel = `Name_Arztpraxis` (Fallback: Adresse, letzter Fallback: Koordinate), um Mehrfachnennungen derselben Praxis/Filiale nicht doppelt zu zählen.
3) Apotheken mit mindestens 2 unterschiedlichen Praxen im Umkreis von 100 m werden als
   `is_med_center = True` markiert. Diese gelten als "medizinisches Zentrum" nach Anforderung.
4) Ergebnis ist ein GeoDataFrame `apotheken_tagged` mit u. a.:
   - `arzt_count_100m`: Anzahl unterschiedlicher Praxen im 100‑m-Umkreis
   - `is_med_center`: True/False, ob die Apotheke als Zentrum zählt
   - Geometrie/Metadaten der Apotheke

WICHTIG: Dieses Skript berechnet KEINE fußläufigen Entfernungen von Wohnadressen zu diesen Zentren.
Die Distanz-/Erreichbarkeitslogik (500 m / 800 m / kürzester Fußweg) wird NICHT mehr hier gemacht,
sondern ausschließlich in einem separaten Routing-Skript gegen den lokalen ORS.
Damit liegt die Gehweg-Berechnung zentral an genau einer Stelle.

Encoding-Hinweise:
- CSV-Encoding wird robust versucht (utf-8, cp1252, latin1).
- Ärzte-CSV: Semikolon-getrennt; Apotheken-CSV: Komma-getrennt (konfigurierbar).
- Spalten mit Umlaut-/Encoding-Artefakten werden normalisiert (z. B. "Stra�enname" → "Strassenname").

Output dieses Skripts:
- `out/apotheken_med_center.csv` / `.parquet`
  Enthält alle Apotheken, plus:
    - `arzt_count_100m`
    - `arzt_keys_100m`
    - `is_med_center`
Diese Datei ist die Eingabe für den ORS-Schritt.
"""

from __future__ import annotations

from typing import Iterable, Optional

import numpy as np
import pandas as pd
import geopandas as gpd

# ----------------------
# Config
# ----------------------
CRS_EPSG = 25833  # UTM 33N, für Brandenburg

# ----------------------
# Utilities
# ----------------------

def _smart_read_csv(path: str, sep: str, encodings: Iterable[str] = ("utf-8", "cp1252", "latin1"), **kwargs) -> pd.DataFrame:
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            return pd.read_csv(path, sep=sep, encoding=enc, **kwargs)
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
    raise RuntimeError(f"Failed to read {path} with encodings {encodings}: {last_err}")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Bereinigt Spaltennamen (Umlaute/� -> ASCII) und vereinheitlicht einige Schlüsselspalten."""
    mapping = {
        "Straßenname": "Strassenname",
        "Straßennamen": "Strassennamen",
        "Straßenschlüssel": "Strassenschluessel",
        "Gemeindename/Ort": "Gemeindename_Ort",
        "HsnrZus": "Hsnr_Zus",
        # fallback für bereits kaputte Umlaute (�)
        "Stra�enname": "Strassenname",
        "Stra�ennamen": "Strassennamen",
        "Stra�enschl�ssel": "Strassenschluessel",
        # Ärzte-spezifisch
        "Name_Arztpraxis": "Name_Arztpraxis",
        "Name_Arzt": "Name_Arzt",
        "Fachrichtung": "Fachrichtung",
        # Apotheke-spezifisch
        "Name_Apotheke": "Name_Apotheke",
    }
    new_cols = []
    for c in df.columns:
        c2 = c.strip()
        c2 = mapping.get(c2, c2)
        new_cols.append(c2)
    df.columns = new_cols
    return df


def _to_gdf(df: pd.DataFrame, x_col: str = "x", y_col: str = "y", crs_epsg: int = CRS_EPSG) -> gpd.GeoDataFrame:
    if x_col not in df.columns or y_col not in df.columns:
        raise KeyError(f"x/y columns '{x_col}', '{y_col}' not found in DataFrame. Columns: {list(df.columns)}")
    gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df[x_col].astype(float), df[y_col].astype(float)),
        crs=f"EPSG:{crs_epsg}",
    )
    return gdf


# ----------------------
# Loaders
# ----------------------

def load_aerzte_csv(path: str, sep: str = ";") -> gpd.GeoDataFrame:
    df = _smart_read_csv(path, sep=sep, dtype=str)
    df = _normalize_columns(df)
    # Typisierung/Konvertierung
    for col in ("x", "y"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # Fallback-Key je Praxis (zur Distinct-Zählung)
    if "Name_Arztpraxis" in df.columns and df["Name_Arztpraxis"].notna().any():
        df["praxis_key"] = df["Name_Arztpraxis"].fillna("").str.strip().replace({"": np.nan})
    elif "Adresse" in df.columns and df["Adresse"].notna().any():
        df["praxis_key"] = df["Adresse"].fillna("").str.strip().replace({"": np.nan})
    else:
        # Koordinate als Key (weniger schön, aber besser als nichts)
        df["praxis_key"] = df[["x", "y"]].astype(str).agg("|".join, axis=1)
    gdf = _to_gdf(df)
    return gdf


def load_apotheken_csv(path: str, sep: str = ",") -> gpd.GeoDataFrame:
    df = _smart_read_csv(path, sep=sep, dtype=str)
    df = _normalize_columns(df)
    # Apotheken-ID sicherstellen
    if "id" not in df.columns:
        # Erzeuge stabile ID
        df["id"] = pd.util.hash_pandas_object(
            df[[c for c in df.columns if c in ("Name_Apotheke", "PLZ", "Strassenname", "Hsnr", "x", "y")]],
            index=False,
        ).astype(str)
    # Typisierung/Konvertierung
    for col in ("x", "y"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    gdf = _to_gdf(df)
    return gdf


# ----------------------
# Core logic: Zentrenbildung
# ----------------------

def build_med_centers(gdf_aerzte: gpd.GeoDataFrame, gdf_apotheken: gpd.GeoDataFrame, radius_m: float = 100.0) -> gpd.GeoDataFrame:
    """Zählt *distinct* Praxen je Apotheke im gegebenen Radius und markiert Zentren.

    Rückgabe: gdf_apotheken mit Spalten
      - arzt_keys_100m (list[str]): Liste der erkannten Praxen-Schlüssel in 100 m
      - arzt_count_100m (int): Anzahl unterschiedlicher Praxen in 100 m
      - is_med_center (bool): True, wenn arzt_count_100m >= 2
    """
    # Sicherstellen, dass beide im selben CRS sind
    if gdf_aerzte.crs != gdf_apotheken.crs:
        gdf_aerzte = gdf_aerzte.to_crs(gdf_apotheken.crs)

    # 100m-Buffer um Apotheken
    apo_buf = gdf_apotheken[["id", "geometry"]].copy()
    apo_buf["geometry"] = apo_buf.buffer(radius_m)

    # Räumlicher Join: Ärzte in Buffer-Polygonen
    joined = gpd.sjoin(
        gdf_aerzte[["praxis_key", "geometry"]],
        apo_buf,
        how="left",
        predicate="within",
    )

    # Gruppieren je Apotheke: distinct Praxis_keys sammeln
    grp = joined.groupby("id")["praxis_key"].agg(
        lambda x: sorted(pd.Series(x).dropna().unique().tolist())
    )

    ap = gdf_apotheken.copy()
    ap = ap.merge(grp.rename("arzt_keys_100m").to_frame(), on="id", how="left")

    ap["arzt_keys_100m"] = ap["arzt_keys_100m"].apply(lambda v: v if isinstance(v, list) else [])
    ap["arzt_count_100m"] = ap["arzt_keys_100m"].apply(len).astype(int)
    ap["is_med_center"] = ap["arzt_count_100m"] >= 2
    return ap


# ----------------------
# Workflow
# ----------------------
if __name__ == "__main__":
    # 1) Laden der Rohdaten
    aerzte = load_aerzte_csv("data/2025_Ärzte.csv", sep=";")
    apotheken = load_apotheken_csv("data/2025_Apotheken.csv", sep=",")

    # 2) Zentrenbildung
    apotheken_tagged = build_med_centers(aerzte, apotheken, radius_m=100.0)
    print("Apotheken gesamt:", len(apotheken_tagged))
    print("Davon medizinische Zentren:", int(apotheken_tagged["is_med_center"].sum()))

    # Füge lat/lon-Spalten hinzu
    apotheken_tagged_wgs84 = apotheken_tagged.to_crs(epsg=4326)
    apotheken_tagged_wgs84["lat"] = apotheken_tagged_wgs84.geometry.y
    apotheken_tagged_wgs84["lon"] = apotheken_tagged_wgs84.geometry.x

    # 3) Export für Routing-Schritt (mit Konvertierung zu WGS84)
    apotheken_tagged_wgs84.to_csv("out/medzentren_geocoded.csv", index=False)
