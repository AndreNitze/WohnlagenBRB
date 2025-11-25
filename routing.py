import pandas as pd
import numpy as np
import requests
import json
import os
from collections import defaultdict

# ---------------------------------------------------------
# Konfiguration
ORS_URL = "http://localhost:8080/ors/v2/directions/foot-walking/geojson"  # Lokale ORS-Instanz (Docker o.ä.)
#ORS_URL = "https://api.openrouteservice.org/v2/directions/foot-walking/geojson"
ORS_API_KEY = "your_api_key_here"  # Falls externe ORS-API genutzt wird
CSV_ADDRESSES     = "out/adressen_geocoded.csv"
CSV_DESTINATIONS  = "out/einzelhandel_geocoded.csv"
CSV_OUTPUT        = "out/adressen_mit_einzelhandel_routen.csv"

DISTANCE_THRESHOLDS = [500, 800, 1000]  # Meter
routing = True
use_haversine = True
HAVERSINE_LIMIT_M = 2000
# ---------------------------------------------------------

def extract_domain(file_path):
    filename = os.path.basename(file_path)
    parts = filename.replace(".csv", "").split("_")
    for part in parts:
        if part.lower() not in ["geocoded", "standorte", "data"]:
            return part.lower()
    return "ziel"

DOMAIN = extract_domain(CSV_DESTINATIONS)

def haversine_np(lat1, lon1, lat2_array, lon2_array):
    """Vektorisierte Haversine-Berechnung (Meter)"""
    R = 6371000  # Erd-Radius in m
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2_array)
    d_phi = phi2 - phi1
    d_lambda = np.radians(lon2_array - lon1)

    a = np.sin(d_phi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(d_lambda/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def route_distance(lon_start, lat_start, lon_dest, lat_dest):
    """Returns (distance_m, geometry_LineString_JSON) or (None, None)"""
    coords = [[float(lon_start), float(lat_start)],
              [float(lon_dest),  float(lat_dest)]]
    payload = {
        "coordinates": coords,
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
            timeout=10
        )
        r.raise_for_status()
        feature = r.json()["features"][0]
        distance = feature["properties"]["summary"]["distance"]
        geometry = json.dumps(feature["geometry"])
        return distance, geometry
    except Exception as e:
        print("--- ORS Request fehlgeschlagen ---")
        print("Start:", lat_start, lon_start, "| Ziel:", lat_dest, lon_dest)
        print(e)
        return None, None

# ---------------------------------------------------------
# Daten laden
df_addr = pd.read_csv(CSV_ADDRESSES)
df_dest = pd.read_csv(CSV_DESTINATIONS)

df_addr = df_addr[df_addr["lat"].notna() & df_addr["lon"].notna()].copy()
df_dest = df_dest[df_dest["lat"].notna() & df_dest["lon"].notna()].copy()

# Sonderfall "Medizinische Zentren": Filtere Einzel-Apotheken raus
if "is_med_center" in df_dest.columns:
    df_dest["is_med_center"] = (df_dest["is_med_center"].astype(str).str.lower().isin(["true", "1", "yes", "y"]))
    df_dest = df_dest[df_dest["is_med_center"] == True].copy()

# ---------------------------------------------------------
# Routing
nearest_distances = []
nearest_geometries = []
distance_counters = defaultdict(list)

# Zielpunkte vorbereiten
lats_d = df_dest["lat"].to_numpy()
lons_d = df_dest["lon"].to_numpy()

for i, addr in df_addr.iterrows():
    lat_a, lon_a = float(addr["lat"]), float(addr["lon"])
    min_dist = None
    min_geom = None
    counter = dict.fromkeys(DISTANCE_THRESHOLDS, 0)

    if routing:
        candidates = df_dest.copy()

        if use_haversine:
            distances = haversine_np(lat_a, lon_a, lats_d, lons_d)
            mask = distances <= HAVERSINE_LIMIT_M
            near_candidates = df_dest[mask]

            # Wenn nichts im Umkreis HAVERSINE_LIMIT_M liegt, trotzdem gegen ALLE Ziele routen
            if len(near_candidates) > 0:
                candidates = near_candidates
            else:
                candidates = df_dest  # <- Fallback

        # Routings zu allen Kandidaten durchführen
        for _, dest in candidates.iterrows():
            dist_m, geom = route_distance(
                lon_a, lat_a,
                float(dest["lon"]), float(dest["lat"])
            )
            if dist_m is None:
                continue

            # Vorkommen innerhalb der Schwellen zählen
            for d in DISTANCE_THRESHOLDS:
                if dist_m <= d:
                    counter[d] += 1

            # Nächstgelegenes Ziel merken
            if min_dist is None or dist_m < min_dist:
                min_dist = dist_m
                min_geom = geom
    else:
        # Kein Routing
        min_dist = None
        min_geom = None
        counter = dict.fromkeys(DISTANCE_THRESHOLDS, None)

    nearest_distances.append(min_dist)
    nearest_geometries.append(min_geom)

    for d in DISTANCE_THRESHOLDS:
        distance_counters[d].append(counter[d])

    if (i + 1) % 10 == 0:
        print(f"{i + 1}/{len(df_addr)} Adressen verarbeitet")

# ---------------------------------------------------------
# Speichern
df_addr[f"{DOMAIN}_min_distance_m"] = nearest_distances
df_addr[f"{DOMAIN}_route"] = nearest_geometries
for d in DISTANCE_THRESHOLDS:
    df_addr[f"{DOMAIN}_count_within_{d}m"] = distance_counters[d]

df_addr.to_csv(CSV_OUTPUT, index=False)
print(f"✓ Datei geschrieben: {CSV_OUTPUT}")
