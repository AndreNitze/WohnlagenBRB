import pandas as pd
import numpy as np
import requests
import json
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points

# ---------------------------------------------------------
# MODUS-KONFIGURATION
# ---------------------------------------------------------
# ROUTING_MODE = "poi"     # Routing zu Punktzielen
ROUTING_MODE = "area"      # Routing zu Grünflächen (Polygonflächen)

# Wenn AREA:
AREA_PATH = "data/Grünflächen_Verkehrszeichen/20251029_Vegetation_KSP_GP_31.shp"

# ---------------------------------------------------------
# ORS KONFIGURATION
# ---------------------------------------------------------
ORS_URL     = "http://localhost:8080/ors/v2/directions/foot-walking/geojson"
ORS_API_KEY = "your_api_key_here"

# ---------------------------------------------------------
# DATEIEN
# ---------------------------------------------------------
DOMAIN            = "gruen"
CSV_ADDRESSES     = "out/adressen_geocoded.csv"
CSV_DESTINATIONS  = "out/" + DOMAIN + "_geocoded.csv"   # nur POI-Modus
CSV_OUTPUT        = "out/adressen_mit_" + DOMAIN + "_routen.csv"

# ---------------------------------------------------------
# PARAMETER
# ---------------------------------------------------------
DISTANCE_THRESHOLDS = [500, 800, 1000]     # Zählradius
HAVERSINE_LIMIT_M   = 2000                 # Kandidatensuche (aktuell nicht verwendet)
MAX_WORKERS         = 16                   # parallele Threads
ROUTING_ENABLED     = True                 # debug/skip möglich

# ---------------------------------------------------------
# DEBUG-COUNTER
# ---------------------------------------------------------
ROUTE_SUCCESS_COUNT = 0
ROUTE_ERROR_COUNT   = 0
ROUTE_ERROR_LIMIT   = 10    # max. 10 Fehler detailliert ausgeben

# ---------------------------------------------------------
# FUNKTIONEN
# ---------------------------------------------------------

def haversine_np(lat1, lon1, lat2, lon2):
    """Vektorisierte Haversine-Distanz (Meter)."""
    R = 6371000
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    d_phi = phi2 - phi1
    d_lam = np.radians(lon2 - lon1)
    a = np.sin(d_phi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(d_lam/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))


def route_distance(lon_a, lat_a, lon_b, lat_b):
    """ORS Fußrouting: gibt (dist_m, Liniengeometrie_json) zurück."""
    global ROUTE_SUCCESS_COUNT, ROUTE_ERROR_COUNT

    # Guard: falls Koordinaten None/NaN sind, Routing überspringen
    if lon_a is None or lat_a is None or lon_b is None or lat_b is None:
        return None, None
    if any(np.isnan([lon_a, lat_a, lon_b, lat_b])):
        return None, None

    if not ROUTING_ENABLED:
        # Distanzberechnung kann zum Debuggen abgeschaltet werden
        return None, None

    payload = {
        "coordinates": [[lon_a, lat_a], [lon_b, lat_b]],
        "instructions": False,
        "geometry": True,
        "preference": "recommended"
    }

    try:
        r = requests.post(
            ORS_URL,
            data=json.dumps(payload),
            headers={
                "Authorization": ORS_API_KEY,
                "Content-Type": "application/json"
            },
            timeout=15
        )

        if r.status_code != 200:
            ROUTE_ERROR_COUNT += 1
            if ROUTE_ERROR_COUNT <= ROUTE_ERROR_LIMIT:
                print(f"[ORS-ERROR] Status: {r.status_code}")
                # nur die ersten ~300 Zeichen, damit Console nicht explodiert
                print(f"[ORS-ERROR] Antwort: {r.text[:300]}")
                print(f"[ORS-ERROR] Payload: {payload}")
            return None, None

        data = r.json()
        if "features" not in data or not data["features"]:
            ROUTE_ERROR_COUNT += 1
            if ROUTE_ERROR_COUNT <= ROUTE_ERROR_LIMIT:
                print("[ORS-ERROR] Keine 'features' im Response.")
                print(f"[ORS-ERROR] Antwort: {str(data)[:300]}")
                print(f"[ORS-ERROR] Payload: {payload}")
            return None, None

        feat = data["features"][0]
        dist = feat["properties"]["summary"]["distance"]
        geom = json.dumps(feat["geometry"])

        ROUTE_SUCCESS_COUNT += 1
        if ROUTE_SUCCESS_COUNT <= 5:
            print(f"[ORS-OK] Beispiel-Distanz: {dist} m")

        return dist, geom

    except Exception as e:
        ROUTE_ERROR_COUNT += 1
        if ROUTE_ERROR_COUNT <= ROUTE_ERROR_LIMIT:
            print(f"[ORS-EXCEPTION] {type(e).__name__}: {e}")
            print(f"[ORS-EXCEPTION] Payload: {payload}")
        return None, None


def nearest_entry_point(point_wgs84, area_geom):
    """
    Berechnet robust einen Eintrittspunkt für EINE Grünfläche (Polygon oder MultiPolygon).
    Gibt (lon, lat) in WGS84 zurück oder (None, None), falls nicht möglich.
    """
    try:
        # Punkt -> Meter (UTM Zone 33N)
        point_m = gpd.GeoSeries([point_wgs84], crs=4326).to_crs(32633).iloc[0]
    except Exception:
        return None, None

    if area_geom is None or area_geom.empty:
        return None, None

    geom = area_geom.geometry.iloc[0]
    if geom is None or geom.is_empty:
        return None, None

    # Multipolygon abfangen: größtes Polygon wählen
    if geom.geom_type == "MultiPolygon":
        if len(geom.geoms) == 0:
            return None, None
        geom = max(geom.geoms, key=lambda g: g.area)

    # ungültige Geometrien reparieren
    if not geom.is_valid:
        geom = geom.buffer(0)
        if geom.is_empty:
            return None, None

    # boundary extrahieren
    boundary = geom.boundary
    if boundary.is_empty:
        # Fallback: nächster Punkt auf Fläche statt auf Boundary
        try:
            nearest = nearest_points(point_m, geom)[1]
            entry_wgs = gpd.GeoSeries([nearest], crs=32633).to_crs(4326).iloc[0]
            lon = float(entry_wgs.x)
            lat = float(entry_wgs.y)
            if np.isnan(lon) or np.isnan(lat):
                return None, None
            return lon, lat
        except Exception:
            return None, None

    try:
        proj = boundary.project(point_m)
        entry_m = boundary.interpolate(proj)

        if entry_m.is_empty:
            return None, None

        entry_wgs = gpd.GeoSeries([entry_m], crs=32633).to_crs(4326).iloc[0]
        lon = float(entry_wgs.x)
        lat = float(entry_wgs.y)
        if np.isnan(lon) or np.isnan(lat):
            return None, None
        return lon, lat
    except Exception:
        return None, None


def route_task(args):
    """ThreadPool-Routing."""
    lon_a, lat_a, lon_d, lat_d, addr_idx, area_id = args
    dist, geom = route_distance(lon_a, lat_a, lon_d, lat_d)
    return addr_idx, area_id, dist, geom


# ---------------------------------------------------------
# DATEN LADEN: ADRESSEN
# ---------------------------------------------------------
df_addr = pd.read_csv(CSV_ADDRESSES)
df_addr = df_addr[df_addr["lat"].notna() & df_addr["lon"].notna()].copy()

# ---------------------------------------------------------
# DATEN LADEN: ZIELPUNKTE (nur POI-Modus)
# ---------------------------------------------------------
if ROUTING_MODE == "poi":
    if not os.path.exists(CSV_DESTINATIONS):
        raise FileNotFoundError(f"POI-Modus aktiv, aber Datei nicht gefunden: {CSV_DESTINATIONS}")

    df_dest = pd.read_csv(CSV_DESTINATIONS)
    df_dest = df_dest[df_dest["lat"].notna() & df_dest["lon"].notna()].copy()

    lats_dest = df_dest["lat"].to_numpy()
    lons_dest = df_dest["lon"].to_numpy()

else:
    df_dest = None


# ---------------------------------------------------------
# DATEN LADEN: GRÜNFLÄCHEN (nur AREA-Modus)
# ---------------------------------------------------------
if ROUTING_MODE == "area":

    print("→ Lade Grünflächen…")

    if not os.path.exists(AREA_PATH):
        raise FileNotFoundError(f"AREA_PATH nicht gefunden: {AREA_PATH}")

    gdf_gruen = gpd.read_file(AREA_PATH)

    # Dissolve nach objektbeze
    gdf_area = gdf_gruen.to_crs(32633).dissolve(by="objektbeze").reset_index()

    # Debug Geometrien
    print("→ Prüfe Geometrien nach dissolve()...")
    print("Anzahl ungültiger Geometrien:", sum(~gdf_area.geometry.is_valid))
    print("Anzahl leerer Geometrien:", sum(gdf_area.geometry.is_empty))
    print("Geom-Typ-Übersicht:")
    print(gdf_area.geometry.geom_type.value_counts())

    # Centroid für Kandidatensuche
    gdf_area["centroid_wgs"] = gdf_area.geometry.centroid.to_crs(4326)

    area_ids      = gdf_area["objektbeze"].tolist()
    area_cent_lat = gdf_area["centroid_wgs"].y.to_numpy()
    area_cent_lon = gdf_area["centroid_wgs"].x.to_numpy()

    print(f"✓ {len(gdf_area)} Grünflächen-Gruppen geladen.")

else:
    gdf_area = None


# ---------------------------------------------------------
# ROUTING-TASKS ERZEUGEN
# ---------------------------------------------------------
tasks = []   # (lon_a, lat_a, lon_d, lat_d, address_index, area_id)
print("→ Erzeuge Routing-Tasks…")

for addr_idx, row in df_addr.iterrows():

    lat_a = float(row["lat"])
    lon_a = float(row["lon"])
    pt    = Point(lon_a, lat_a)

    # -----------------------------------------------------
    # POI-MODUS
    # -----------------------------------------------------
    if ROUTING_MODE == "poi":
        dists = haversine_np(lat_a, lon_a, lats_dest, lons_dest)
        min_pos = np.argmin(dists)

        lat_d = float(lats_dest[min_pos])
        lon_d = float(lons_dest[min_pos])

        tasks.append((lon_a, lat_a, lon_d, lat_d, addr_idx, "poi_target"))

    # -----------------------------------------------------
    # AREA-MODUS – mehrere GRÜNFLÄCHEN pro Adresse
    # -----------------------------------------------------
    elif ROUTING_MODE == "area":

        # Schritt 1: Luftlinien-Kandidatensuche
        dists = haversine_np(lat_a, lon_a, area_cent_lat, area_cent_lon)

        cand_mask = dists <= 1500
        if not cand_mask.any():
            nearest_idx = np.argmin(dists)
            cand_mask[nearest_idx] = True

        cand_areas = gdf_area[cand_mask]

        # Schritt 2: Für jede Kandidaten-Grünfläche → Entry Point + Task
        for _, area_row in cand_areas.iterrows():

            area_id = area_row["objektbeze"]
            area_geom = gdf_area[gdf_area["objektbeze"] == area_id]

            lon_d, lat_d = nearest_entry_point(pt, area_geom)

            if lon_d is None or lat_d is None:
                # Nur für Diagnose:
                # print(f"[WARN] Kein Entry für Fläche {area_id} bei Adresse {addr_idx}.")
                continue

            tasks.append((lon_a, lat_a, lon_d, lat_d, addr_idx, area_id))

print("Anzahl Routing-Tasks:", len(tasks))

# ---------------------------------------------------------
# PARALLELES ROUTING
# ---------------------------------------------------------
print(f"→ Starte paralleles Routing ({MAX_WORKERS} Threads)…")
results = defaultdict(dict)

if ROUTING_ENABLED and len(tasks) > 0:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_list = [ex.submit(route_task, t) for t in tasks]

        for fut in as_completed(future_list):
            addr_idx, area_id, dist, geom = fut.result()
            # dist kann None sein, wir speichern trotzdem
            results[addr_idx][area_id] = (dist, geom)

print("✓ Routing abgeschlossen.")
print("ORS erfolgreiche Anfragen:", ROUTE_SUCCESS_COUNT)
print("ORS Fehlanfragen:", ROUTE_ERROR_COUNT)
print("Anzahl Adressen im results-Objekt:", len(results))
print(
    "Adressen mit mindestens einer Distanz (dist != None):",
    sum(
        any(d is not None for d in dist_map.values())
        for dist_map in results.values()
    )
)

# ---------------------------------------------------------
# ERGEBNISSE ZUSAMMENFÜHREN
# ---------------------------------------------------------
df_addr[DOMAIN + "_min_distance_m"] = None
df_addr[DOMAIN + "_route"] = None

for d in DISTANCE_THRESHOLDS:
    df_addr[f"{DOMAIN}_count_within_{d}m"] = 0

for addr_idx in df_addr.index:
    dist_map = results.get(addr_idx, {})

    if len(dist_map) == 0:
        continue

    # Für Statistik: nur Distanzen extrahieren
    distances_only = [
        dist_geom[0]
        for dist_geom in dist_map.values()
        if dist_geom[0] is not None
    ]

    if len(distances_only) == 0:
        continue

    # Für beste Route: komplette Einträge
    valid_entries = [
        (area_id, dist_geom[0], dist_geom[1])
        for area_id, dist_geom in dist_map.items()
        if dist_geom[0] is not None
    ]

    # Minimale Distanz finden
    best_area, best_dist, best_geom = min(valid_entries, key=lambda x: x[1])

    # Eintragen
    df_addr.loc[addr_idx, DOMAIN + "_min_distance_m"] = best_dist
    df_addr.loc[addr_idx, DOMAIN + "_route"] = best_geom

    # Count innerhalb thresholds
    for d in DISTANCE_THRESHOLDS:
        df_addr.loc[addr_idx, f"{DOMAIN}_count_within_{d}m"] = sum(
            dist <= d for dist in distances_only
        )


# ---------------------------------------------------------
# SPEICHERN
# ---------------------------------------------------------
df_addr.to_csv(CSV_OUTPUT, index=False)
print(f"✓ Datei geschrieben: {CSV_OUTPUT}")
