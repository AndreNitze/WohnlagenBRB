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
# CONFIG
# ---------------------------------------------------------
ROUTING_MODE = "area"
ORS_URL      = "http://localhost:8080/ors/v2/directions/foot-walking/geojson"
ORS_API_KEY  = "your_api_key_here"

DOMAIN            = "gruen"
CSV_ADDRESSES     = "out/adressen_geocoded.csv"
AREA_PATH         = "data/Grünflächen_Verkehrszeichen/20251029_Vegetation_KSP_GP_31.shp"
CSV_OUTPUT        = "out/adressen_mit_" + DOMAIN + "_routen.csv"

DISTANCE_THRESHOLDS = [500, 800, 1000]
MAX_WORKERS         = 16
ROUTING_ENABLED     = True

# ---------------------------------------------------------
# ROUTING LOGIC WITH FULL FALLBACKS
# ---------------------------------------------------------

def haversine_single(lat1, lon1, lat2, lon2):
    """Haversine Distanz zwischen zwei Punkten."""
    R = 6371000
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = phi2 - phi1
    dl   = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dl/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))


def route_distance(lon_a, lat_a, lon_d, lat_d):
    """Versuch ORS-Fußroute zu berechnen. Fallback: None."""
    if any(pd.isna([lon_a, lat_a, lon_d, lat_d])):
        return None, None

    payload = {
        "coordinates": [[lon_a, lat_a], [lon_d, lat_d]],
        "instructions": False,
        "geometry": True,
        "preference": "recommended"
    }

    try:
        r = requests.post(
            ORS_URL,
            data=json.dumps(payload),
            headers={"Authorization": ORS_API_KEY, "Content-Type": "application/json"},
            timeout=8
        )
        if r.status_code != 200:
            return None, None
        data = r.json()
        if "features" not in data or not data["features"]:
            return None, None

        feat = data["features"][0]
        dist = feat["properties"]["summary"]["distance"]
        geom = json.dumps(feat["geometry"])
        return dist, geom

    except:
        return None, None


def entry_point_with_fallback(pt_wgs84, area_geom, centroid_wgs):
    """
    Robust: Entry-Point → wenn nicht möglich → nimm den Flächen-Zentroid (Garantiert verfügbar).
    """
    try:
        # Versuch: Projektion nach Meter
        pt_m = gpd.GeoSeries([pt_wgs84], crs=4326).to_crs(32633).iloc[0]
        geom = area_geom.geometry.iloc[0]

        if geom.geom_type == "MultiPolygon":
            geom = max(geom.geoms, key=lambda g: g.area)

        if not geom.is_valid:
            geom = geom.buffer(0)

        boundary = geom.boundary
        if boundary.is_empty:
            raise Exception("Boundary empty")

        proj = boundary.project(pt_m)
        entry_m = boundary.interpolate(proj)
        entry_wgs = (
            gpd.GeoSeries([entry_m], crs=32633).to_crs(4326).iloc[0]
        )
        return float(entry_wgs.x), float(entry_wgs.y)

    except:
        # FALLBACK: Zentroid
        return float(centroid_wgs.x), float(centroid_wgs.y)


# ---------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------

df_addr = pd.read_csv(CSV_ADDRESSES)
df_addr = df_addr[df_addr["lat"].notna() & df_addr["lon"].notna()].copy()
df_addr.reset_index(drop=True, inplace=True)

print("Adressen geladen:", len(df_addr))

# GRÜNFLÄCHEN
gdf_raw = gpd.read_file(AREA_PATH)
gdf_area = gdf_raw.to_crs(32633).dissolve(by="objektbeze").reset_index()
gdf_area["centroid_wgs"] = gdf_area.geometry.centroid.to_crs(4326)

area_cent_lat = gdf_area["centroid_wgs"].y.to_numpy()
area_cent_lon = gdf_area["centroid_wgs"].x.to_numpy()

print("Flächen:", len(gdf_area))

# ---------------------------------------------------------
# TASK GENERATION
# ---------------------------------------------------------

tasks = []
for addr_idx, r in df_addr.iterrows():
    lat = r.lat
    lon = r.lon
    pt  = Point(lon, lat)

    # Kandidatensuche per Luftlinie
    dists = haversine_single(lat, lon, area_cent_lat, area_cent_lon)
    nearest_idx = np.argmin(dists)

    # Wir verwenden ausschließlich *eine* Fläche: die nächstgelegene.
    area_row  = gdf_area.iloc[nearest_idx]
    centroid  = area_row["centroid_wgs"]
    area_geom = gdf_area[gdf_area["objektbeze"] == area_row["objektbeze"]]

    # Best Entry Point (mit Zentroid-Fallback)
    lon_d, lat_d = entry_point_with_fallback(pt, area_geom, centroid)

    tasks.append((lon, lat, lon_d, lat_d, addr_idx, area_row["objektbeze"]))

print("Routing-Tasks:", len(tasks))

# ---------------------------------------------------------
# PARALLEL ROUTING
# ---------------------------------------------------------

results = {}
def worker(t):
    lon_a, lat_a, lon_d, lat_d, idx, area_id = t
    dist, geom = route_distance(lon_a, lat_a, lon_d, lat_d)

    # HARDCODED FALLBACK
    if dist is None:
        dist = haversine_single(lat_a, lon_a, lat_d, lon_d)
        geom = None

    return idx, dist, geom, area_id

with ThreadPoolExecutor(MAX_WORKERS) as ex:
    futures = [ex.submit(worker, t) for t in tasks]
    for fut in as_completed(futures):
        idx, dist, geom, area_id = fut.result()
        results[idx] = (dist, geom, area_id)

print("Routing fertig.")

# ---------------------------------------------------------
# MERGE RESULTS (JEDE ADRESSE BEKOMMT EIN ERGEBNIS)
# ---------------------------------------------------------

df_addr["gruen_min_distance"] = None
df_addr["gruen_route"]        = None
df_addr["gruen_area_id"]      = None

for idx, (dist, geom, area_id) in results.items():
    df_addr.loc[idx, "gruen_min_distance"] = dist
    df_addr.loc[idx, "gruen_route"]        = geom
    df_addr.loc[idx, "gruen_area_id"]      = area_id

# Threshold counts
for d in DISTANCE_THRESHOLDS:
    df_addr[f"{DOMAIN}_count_within_{d}m"] = (df_addr["gruen_min_distance"] <= d).astype(int)

df_addr.to_csv(CSV_OUTPUT, index=False)
print("✓ Datei geschrieben:", CSV_OUTPUT)
