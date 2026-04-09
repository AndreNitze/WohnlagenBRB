import pandas as pd
import geopandas as gpd
import folium
from shapely import wkt
from shapely.geometry import Point
import ast

HAUSNUMMERZUSATZ = "HsnrZus"
HAUSNUMMER = "Hsnr"
STRASSENNAME = "Straßenname"

def _first_scalar(v):
    """Gibt aus pandas-Containern den ersten brauchbaren Skalar zurück."""
    if isinstance(v, (pd.Series, pd.Index)):
        for item in v.tolist():
            if not _is_missing(item):
                return item
        return None
    return v

def _is_missing(v) -> bool:
    """Robuster Missing-Check für Skalare und pandas-Objekte."""
    v = _first_scalar(v)
    if v is None:
        return True
    return bool(pd.isna(v))


def _truthy(v) -> bool:
    """Robuster Bool-Check, auch wenn versehentlich eine Series ankommt."""
    if isinstance(v, (pd.Series, pd.Index)):
        return any(_truthy(item) for item in v.tolist())
    v = _first_scalar(v)
    if _is_missing(v):
        return False
    return bool(v)


def make_merge_addr(row):
    s = str(row[STRASSENNAME]).strip().lower()
    hn = str(row[HAUSNUMMER]).strip().lower()
    hzusatz_raw = row.get(HAUSNUMMERZUSATZ, row.get("Hsnrzus", ""))
    hzusatz = str(hzusatz_raw).strip().lower() if not _is_missing(hzusatz_raw) else ""
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
            val = s(row.get(nc, ""))
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
    v = _first_scalar(v)
    if _is_missing(v):
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

def clean_index_cols(df):
    cols = [c for c in df.columns if c.startswith("index_")]
    if cols:
        df.drop(columns=cols, inplace=True, errors="ignore")
    return df

def add_medcenter_markers(map_obj, csv_path, fach_dict, color="red", icon="staff-snake", layer_name="Medizinische Zentren"):
    df = pd.read_csv(csv_path)

    layer = folium.FeatureGroup(name=layer_name)
    layer.add_to(map_obj)

    for _, row in df.iterrows():
        lat, lon = row["lat"], row["lon"]
        if pd.isna(lat) or pd.isna(lon):
            continue

        # Arztliste parsen
        arzt_keys = row.get("arzt_keys_100m", "[]")
        if isinstance(arzt_keys, str):
            arzt_keys = ast.literal_eval(arzt_keys)

        # Popup zusammenbauen
        lines = []

        if _truthy(row.get("is_med_center", False)):
            lines.append(f"<b>Medizinisches Zentrum<br>{row.get('Strassenname','')}</b><br>")

        # Apotheke
        name_ap = str(row.get("Name_Apotheke", "")).strip()
        if name_ap:
            lines.append(f"🏥 {name_ap}<br>")

        # Anzahl Ärzte
        if len(arzt_keys) > 0:
            lines.append(f"<br><b>{len(arzt_keys)} Arztpraxen im 100 m Radius:</b><br>")

        # Ärzte + Fachrichtung
        for arzt in arzt_keys:
            fach = fach_dict.get(arzt, "(Fachrichtung unbekannt)")
            lines.append(f"{arzt} – {fach}<br>")

        popup_html = "".join(lines)

        # Icon wählen
        ico = folium.Icon(color=color, icon=icon, prefix="fa")

        marker = folium.Marker(
            location=[lat, lon],
            tooltip=row.get("Strassenname", "MedZentrum"),
            icon=ico
        ).add_to(layer)

        # Popup breiter machen
        marker.add_child(folium.Popup(popup_html, max_width=450))
