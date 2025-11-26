import pandas as pd
import numpy as np
import requests
import json
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
from shapely.geometry import Point

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
HAVERSINE_LIMIT_M   = 2000                 # Kandidatensuche
MAX_WORKERS          = 40                  # parallele Threads
ROUTING_ENABLED      = True                # debug/skip möglich

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
            headers={"Authorization": ORS_API_KEY,
                     "Content-Type": "application/json"},
            timeout=15
        )
        r.raise_for_status()
        feat = r.json()["features"][0]
        dist = feat["properties"]["summary"]["distance"]
        geom = json.dumps(feat["geometry"])
        return dist, geom
    except Exception as e:
        return None, None

def nearest_entry_point(point_wgs84, area_geom):
    """Berechnet Eintrittspunkt für EINE Grünfläche."""
    point_m = gpd.GeoSeries([point_wgs84], crs=4326).to_crs(32633).iloc[0]
    poly   = area_geom.to_crs(32633).iloc[0]

    entry_m = poly.geometry.boundary.interpolate(
        poly.geometry.boundary.project(point_m)
    )

    entry_wgs = gpd.GeoSeries([entry_m], crs=32633).to_crs(4326).iloc[0]
    return entry_wgs.x, entry_wgs.y


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

    # Centroid für Kandidatensuche
    gdf_area["centroid_wgs"] = gdf_area.geometry.centroid.to_crs(4326)

    area_ids       = gdf_area["objektbeze"].tolist()
    area_cent_lat  = gdf_area["centroid_wgs"].y.to_numpy()
    area_cent_lon  = gdf_area["centroid_wgs"].x.to_numpy()

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
        # Luftlinie → nächste Kandidaten
        dists = haversine_np(lat_a, lon_a, lats_dest, lons_dest)
        min_pos = np.argmin(dists)

        lat_d = lats_dest[min_pos]
        lon_d = lons_dest[min_pos]

        # Wir erzeugen nur *eine* Route (nächster POI)
        tasks.append((lon_a, lat_a, lon_d, lat_d, addr_idx, "poi_target"))

    # -----------------------------------------------------
    # AREA-MODUS – mehrere GRÜNFLÄCHEN pro Adresse
    # -----------------------------------------------------
    elif ROUTING_MODE == "area":

        # Schritt 1: Luftlinien-Kandidatensuche
        dists = haversine_np(lat_a, lon_a, area_cent_lat, area_cent_lon)

        cand_mask = dists <= 1500
        if not cand_mask.any():
            # Wenn nichts nah genug → dennoch die nächste Fläche
            nearest_idx = np.argmin(dists)
            cand_mask[nearest_idx] = True

        cand_areas = gdf_area[cand_mask]

        # Schritt 2: Für jede Kandidaten-Grünfläche → Entry Point + Task
        for _, area_row in cand_areas.iterrows():

            area_id = area_row["objektbeze"]
            area_geom = gdf_area[gdf_area["objektbeze"] == area_id]

            lon_d, lat_d = nearest_entry_point(pt, area_geom)

            tasks.append((lon_a, lat_a, lon_d, lat_d, addr_idx, area_id))


# ---------------------------------------------------------
# PARALLELES ROUTING
# ---------------------------------------------------------
print(f"→ Starte paralleles Routing ({MAX_WORKERS} Threads)…")
results = defaultdict(dict)

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    future_list = [ex.submit(route_task, t) for t in tasks]

    for fut in as_completed(future_list):
        addr_idx, area_id, dist, geom = fut.result()
        results[addr_idx][area_id] = dist

print("✓ Routing abgeschlossen.")


# ---------------------------------------------------------
# ERGEBNISSE ZUSAMMENFÜHREN
# ---------------------------------------------------------
df_addr[DOMAIN + "_min_distance_m"] = None
df_addr[DOMAIN + "_route"] = None  # optional: nur die Route zum nächsten

# Schwellenwertspalten initialisieren
for d in DISTANCE_THRESHOLDS:
    df_addr[f"{DOMAIN}_count_within_{d}m"] = 0


for addr_idx in df_addr.index:

    dist_map = results.get(addr_idx, {})

    if len(dist_map) == 0:
        continue

    # Liste aller Distanzen
    distances = [v for v in dist_map.values() if v is not None]

    if len(distances) == 0:
        continue

    # nächste Grünfläche bestimmen
    min_dist = min(distances)
    df_addr.loc[addr_idx, DOMAIN + "_min_distance_m"] = min_dist

    # Counter füllen
    for d in DISTANCE_THRESHOLDS:
        df_addr.loc[addr_idx, f"{DOMAIN}_count_within_{d}m"] = sum(dist <= d for dist in distances)


# ---------------------------------------------------------
# SPEICHERN
# ---------------------------------------------------------
df_addr.to_csv(CSV_OUTPUT, index=False)
print(f"✓ Datei geschrieben: {CSV_OUTPUT}")
