import pandas as pd, requests, json, math

# ------------------------------------------------------------------
#  KONSTANTEN
ORS_URL       = "http://localhost:8080/ors/v2/directions/foot-walking/geojson"
PRE_FILTER_KM = 2.0       # Haversine‑Radius, ab dem wir überhaupt routen

# ------------------------------------------------------------------
# Helfer: Haversine‑Distanz (grobe Luftlinie ≈ Meter)
"""
Calculate the Haversine distance between two points on the Earth.

Parameters:
    lat1 (float): Latitude of the first point in decimal degrees.
    lon1 (float): Longitude of the first point in decimal degrees.
    lat2 (float): Latitude of the second point in decimal degrees.
    lon2 (float): Longitude of the second point in decimal degrees.

Returns:
    float: Distance between the two points in meters.
"""
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

# ------------------------------------------------------------------
def route_distance(lon_start, lat_start, lon_dest, lat_dest):
    """Returns distance in meters or None."""
    coords = [
        [float(lon_start), float(lat_start)],
        [float(lon_dest),  float(lat_dest)]
    ]
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
            headers={"Content-Type": "application/json"},
            timeout=10
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
df_addr  = pd.read_csv("data/adressen_mit_routen.csv", encoding="utf-8")
df_shops = pd.read_csv("data/einzelhandel_geocoded.csv", encoding="utf-8")

# Nur Adressen mit Koordinaten
df_addr  = df_addr[df_addr["lat"].notna() & df_addr["lon"].notna()].copy()
df_shops = df_shops[df_shops["lat"].notna() & df_shops["lon"].notna()].copy()

# ------------------------------------------------------------------
# 2) Routing‑Loop
nearest_dist   = []   # Meter
shops_500m     = []   # Anzahl
shops_800m     = []   # Anzahl

for i, a in df_addr.iterrows():
    lat_a, lon_a = float(a.lat), float(a.lon)
    best = None
    count_500 = 0
    count_800 = 0

    # Vorfilter über Luftlinie
    cand = []
    for _, s in df_shops.iterrows():
        d_lin = haversine(lat_a, lon_a, float(s.lat), float(s.lon))
        if d_lin <= PRE_FILTER_KM * 1000:          # nur Shops im 2‑km‑Umkreis routen
            cand.append((d_lin, s))

    # Sortieren, nahe zuerst → evtl. früher abbrechen
    for d_lin, s in sorted(cand, key=lambda x: x[0]):
        dist_m = route_distance(
            lon_a, lat_a,
            float(s.lon), float(s.lat)
        )
        if dist_m is None:
            continue

        # Zählen innerhalb Radien
        if dist_m <= 500: count_500 += 1
        if dist_m <= 800: count_800 += 1

        # Nearest
        if best is None or dist_m < best:
            best = dist_m

        # OPTIONAL: abbrechen, wenn Distanz >800 m und wir schon nearest gefunden haben
        if dist_m > 800 and best is not None:
            break

    nearest_dist.append(best)
    shops_500m.append(count_500)
    shops_800m.append(count_800)

    if (i+1) % 500 == 0:
        print(f"{i+1}/{len(df_addr)} Adressen fertig")

# ------------------------------------------------------------------
# 3) Ergebnis anfügen & speichern
df_addr["shop_min_m"]     = nearest_dist
df_addr["shops_500m_ct"]  = shops_500m
df_addr["shops_800m_ct"]  = shops_800m

df_addr.to_csv("data/adressen_mit_einzelhandel.csv", index=False)
print("✓ Datei geschrieben: adressen_mit_einzelhandel.csv")
