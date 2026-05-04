import json
import math

import pandas as pd
import requests

# ------------------------------------------------------------------
# KONSTANTEN
ORS_URL = "http://localhost:8080/ors/v2/directions/foot-walking/geojson"
PRE_FILTER_KM = 3.5       # Haversine-Radius, ab dem wir ueberhaupt routen
CANDIDATE_LIMIT = 30      # begrenzt ORS-Last


# ------------------------------------------------------------------
def haversine(lat1, lon1, lat2, lon2):
    """Grobe Luftlinien-Distanz in Metern."""
    radius_m = 6371000
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * radius_m * math.asin(math.sqrt(a))


# ------------------------------------------------------------------
def route_distance(lon_start, lat_start, lon_dest, lat_dest):
    """Returns distance in meters or None."""
    coords = [
        [float(lon_start), float(lat_start)],
        [float(lon_dest), float(lat_dest)],
    ]
    payload = {
        "coordinates": coords,
        "instructions": False,
        "geometry": True,
        "preference": "recommended",
        "options": {
            "avoid_features": ["ferries"],
        },
    }
    try:
        r = requests.post(
            ORS_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        feat = r.json()["features"][0]
        return feat["properties"]["summary"]["distance"]
    except Exception as e:
        print("--- ORS Request fehlgeschlagen ---")
        print("Payload:", json.dumps(payload))
        print(e)
        return None


# ------------------------------------------------------------------
# 1) CSVs laden
df_addr = pd.read_csv("out/adressen_mit_routen.csv", encoding="utf-8")
df_shops = pd.read_csv("out/einzelhandel_geocoded.csv", encoding="utf-8")

# Nur Adressen mit Koordinaten
df_addr = df_addr[df_addr["lat"].notna() & df_addr["lon"].notna()].copy()
df_shops = df_shops[df_shops["lat"].notna() & df_shops["lon"].notna()].copy()

# ------------------------------------------------------------------
# 2) Routing-Loop
nearest_dist = []   # Meter
shops_500m = []     # Anzahl
shops_800m = []     # Anzahl

for i, a in df_addr.iterrows():
    lat_a, lon_a = float(a.lat), float(a.lon)
    best = None
    count_500 = 0
    count_800 = 0

    # Vorfilter ueber Luftlinie
    cand = []
    for _, s in df_shops.iterrows():
        d_lin = haversine(lat_a, lon_a, float(s.lat), float(s.lon))
        if d_lin <= PRE_FILTER_KM * 1000:
            cand.append((d_lin, s))

    # Sortieren, nahe zuerst
    for d_lin, s in sorted(cand, key=lambda x: x[0])[:CANDIDATE_LIMIT]:
        dist_m = route_distance(
            lon_a, lat_a,
            float(s.lon), float(s.lat),
        )
        if dist_m is None:
            continue

        # Zaehlen innerhalb Radien
        if dist_m <= 500:
            count_500 += 1
        if dist_m <= 800:
            count_800 += 1

        # Nearest
        if best is None or dist_m < best:
            best = dist_m

    nearest_dist.append(best)
    shops_500m.append(count_500)
    shops_800m.append(count_800)

    if (i + 1) % 500 == 0:
        print(f"{i + 1}/{len(df_addr)} Adressen fertig")

# ------------------------------------------------------------------
# 3) Ergebnis anfuegen & speichern
df_addr["shop_min_distance"] = nearest_dist
df_addr["shops_500m_count"] = shops_500m
df_addr["shops_800m_count"] = shops_800m

df_addr.to_csv("data/adressen_mit_einzelhandel.csv", index=False)
print("Datei geschrieben: adressen_mit_einzelhandel.csv")
