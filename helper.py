import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point

HAUSNUMMERZUSATZ = "HsnrZus"
HAUSNUMMER = "Hsnr"
STRASSENNAME = "Straßenname"

def make_merge_addr(row):
    s = str(row[STRASSENNAME]).strip().lower()
    hn = str(row[HAUSNUMMER]).strip().lower()
    hzusatz = str(row.get(HAUSNUMMERZUSATZ, '')).strip().lower() if HAUSNUMMERZUSATZ in row and not pd.isna(row.get(HAUSNUMMERZUSATZ, None)) else ""
    if hzusatz and hzusatz != "nan":
        hn += hzusatz
    adr = f"{s} {hn}".replace("  ", " ").strip()
    return adr

def load_geocsv(path, crs="EPSG:4326", geometry_col="geometry"):
    df = pd.read_csv(path, encoding="utf-8")

    # FALL 1: "geometry" existiert
    if geometry_col in df.columns:
        # Parse vorhandene Werte; falls leer, bleibt es None
        df[geometry_col] = df[geometry_col].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) and x.startswith("POINT") else None
        )
    # FALL 2: "geometry" existiert NICHT
    else:
        if "lon" in df.columns and "lat" in df.columns:
            # Erzeuge komplett neue "geometry"-Spalte
            df[geometry_col] = df.apply(
                lambda row: Point(float(row["lon"]), float(row["lat"]))
                if pd.notna(row["lon"]) and pd.notna(row["lat"]) else None,
                axis=1
            )
        else:
            raise ValueError(
                "Fehlt sowohl 'geometry' als auch ('lat', 'lon')! "
                f"Gefunden: {df.columns.tolist()}"
            )
    # Mach GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry_col, crs=crs)
    return gdf

TOOLTIP_FORMAT = "<b>{Name}</b><br>{Straßenname} {Hsnr}{HsnrZus}"
def add_markers_from_csv(
    map_obj,
    csv_path,
    color="blue",
    icon="info-sign",
    tooltip_format=TOOLTIP_FORMAT,
    fallback_label="Unbekannte Adresse",
    layer_name=None
):
    """
    Fügt Marker aus einer CSV-Datei einer Folium-Karte hinzu.
    Erwartet mindestens Spalten: 'lat', 'lon', 'Straßenname', 'Hsnr' (optional 'HsnrZus').
    Zusätzlich wird eine Spalte verwendet, deren Name mit 'Name_' beginnt (z. B. 'Name_Arztpraxis').
    """
    df = pd.read_csv(csv_path, encoding="utf-8")
    df.columns = [c.strip() for c in df.columns]
    df = df.dropna(subset=["lat", "lon"])

    # Alle potenziellen Namensspalten vorab bestimmen (z. B. Name_Arztpraxis, Name_Apotheke, ...)
    name_cols = [c for c in df.columns if c.startswith("Name_")]

    layer = folium.FeatureGroup(name=layer_name) if layer_name else map_obj

    for _, row in df.iterrows():
        # explizit fehlende Werte ersetzen
        strasse = s(row.get(STRASSENNAME))
        hsnr    = s(row.get(HAUSNUMMER))
        hsnrzus = s(row.get(HAUSNUMMERZUSATZ))
        hat_adresse = any([strasse, hsnr, hsnrzus])

        # Name aus der ersten nicht-leeren 'Name_'-Spalte ableiten
        name_value = ""
        for nc in name_cols:
            val = str(row.get(nc, "") or "").strip()
            if val:
                name_value = val
                break

        # Tooltip bauen (falls keine Adresse, Fallback)
        if hat_adresse or name_value:
            tooltip = tooltip_format.format(
                Name=name_value,
                Straßenname=strasse,
                Hsnr=hsnr,
                HsnrZus=hsnrzus
            )
        else:
            tooltip = fallback_label

        marker = folium.Marker(
            location=[row["lat"], row["lon"]],
            icon=folium.Icon(color=color, icon=icon, prefix="fa"),
            tooltip=tooltip
        )
        marker.add_to(layer)

    if layer_name:
        layer.add_to(map_obj)

def s(v) -> str:
    """NaN/None -> '', sonst getrimmt als String."""
    if v is None or pd.isna(v):
        return ""
    # manche CSVs haben das Literal "nan" als Text:
    if isinstance(v, str) and v.strip().lower() == "nan":
        return ""
    return str(v).strip()

def min_max(series, invert=False):
    s = series.copy()
    if invert:
        s = -s
    return (s - s.min()) / (s.max() - s.min())