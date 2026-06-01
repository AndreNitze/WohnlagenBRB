"""Berechnet Fusswege von allen Wohnadressen zur Jahrtausendbruecke.

Konfiguration:
- `CSV_ADDRESSES` und `CSV_OUTPUT` legen Eingabe und Ergebnisdatei fest.
- `CITY_CENTER_LAT` und `CITY_CENTER_LON` definieren den Zielpunkt.
- `ORS_URL`, `ORS_API_KEY`, `MAX_WORKERS`, `ORS_TIMEOUT_S` und
  `ORS_RETRY_RADII_M` steuern die OpenRouteService-Anfragen.
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests


ORS_URL = "http://localhost:8080/ors/v2/directions/foot-walking/geojson"
ORS_API_KEY = "your_api_key_here"

CSV_ADDRESSES = "out/adressen_geocoded.csv"
CSV_OUTPUT = "out/adressen_mit_zentrum_routen.csv"

CITY_CENTER_LAT = 52.4116351153561
CITY_CENTER_LON = 12.556331280534392

MAX_WORKERS = 8
ORS_TIMEOUT_S = 15
ORS_RETRY_RADII_M = [None, 1200, 2500]
ROUTING_PROGRESS_INTERVAL_S = 5

ROUTE_SUCCESS_COUNT = 0
ROUTE_ERROR_COUNT = 0
ROUTE_ERROR_LIMIT = 10
COUNTER_LOCK = threading.Lock()


def haversine_np(lat1, lon1, lat2, lon2):
    radius_m = 6371000
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    d_phi = phi2 - phi1
    d_lam = np.radians(lon2 - lon1)
    a = np.sin(d_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(d_lam / 2) ** 2
    return radius_m * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def distance_from_linestring_m(geometry):
    if not isinstance(geometry, dict) or geometry.get("type") != "LineString":
        return None

    coords = geometry.get("coordinates")
    if not isinstance(coords, list) or len(coords) == 0:
        return None
    if len(coords) == 1:
        return 0.0

    arr = np.asarray(coords, dtype=float)
    if arr.ndim != 2 or arr.shape[1] < 2:
        return None

    seg_m = haversine_np(arr[:-1, 1], arr[:-1, 0], arr[1:, 1], arr[1:, 0])
    return float(np.sum(seg_m))


def route_to_center(addr_idx, lon_a, lat_a):
    global ROUTE_SUCCESS_COUNT, ROUTE_ERROR_COUNT

    if any(np.isnan([lon_a, lat_a, CITY_CENTER_LON, CITY_CENTER_LAT])):
        return addr_idx, None, None, None

    for radius in ORS_RETRY_RADII_M:
        payload = {
            "coordinates": [[lon_a, lat_a], [CITY_CENTER_LON, CITY_CENTER_LAT]],
            "instructions": False,
            "geometry": True,
            "preference": "recommended",
            "options": {"avoid_features": ["ferries"]},
        }
        if radius is not None:
            payload["radiuses"] = [radius, radius]

        try:
            response = requests.post(
                ORS_URL,
                data=json.dumps(payload),
                headers={"Authorization": ORS_API_KEY, "Content-Type": "application/json"},
                timeout=ORS_TIMEOUT_S,
            )
        except Exception as exc:
            with COUNTER_LOCK:
                ROUTE_ERROR_COUNT += 1
                error_count = ROUTE_ERROR_COUNT
            if error_count <= ROUTE_ERROR_LIMIT:
                print(f"[ORS-EXCEPTION] {type(exc).__name__}: {exc}")
                print(f"[ORS-EXCEPTION] Payload: {payload}")
            return addr_idx, None, None, None

        if response.status_code != 200:
            if response.status_code == 404:
                try:
                    err_code = response.json().get("error", {}).get("code")
                except Exception:
                    err_code = None
                if err_code == 2010 and radius is not ORS_RETRY_RADII_M[-1]:
                    continue

            with COUNTER_LOCK:
                ROUTE_ERROR_COUNT += 1
                error_count = ROUTE_ERROR_COUNT
            if error_count <= ROUTE_ERROR_LIMIT:
                print(f"[ORS-ERROR] Status: {response.status_code}")
                print(f"[ORS-ERROR] Antwort: {response.text[:300]}")
                print(f"[ORS-ERROR] Payload: {payload}")
            return addr_idx, None, None, None

        data = response.json()
        features = data.get("features", [])
        if not features:
            with COUNTER_LOCK:
                ROUTE_ERROR_COUNT += 1
                error_count = ROUTE_ERROR_COUNT
            if error_count <= ROUTE_ERROR_LIMIT:
                print("[ORS-ERROR] Keine 'features' im Response.")
                print(f"[ORS-ERROR] Antwort: {str(data)[:300]}")
            return addr_idx, None, None, None

        feature = features[0]
        summary = feature.get("properties", {}).get("summary", {})
        geometry = feature.get("geometry")
        distance_m = summary.get("distance")
        duration_s = summary.get("duration")

        if distance_m is None:
            distance_m = distance_from_linestring_m(geometry)
        if distance_m is None:
            with COUNTER_LOCK:
                ROUTE_ERROR_COUNT += 1
                error_count = ROUTE_ERROR_COUNT
            if error_count <= ROUTE_ERROR_LIMIT:
                print("[ORS-ERROR] Distanz fehlt im Response und konnte nicht abgeleitet werden.")
            return addr_idx, None, None, None

        with COUNTER_LOCK:
            ROUTE_SUCCESS_COUNT += 1
            success_count = ROUTE_SUCCESS_COUNT
        if success_count <= 5:
            print(f"[ORS-OK] Beispiel-Distanz Zentrum: {distance_m} m")

        return addr_idx, float(distance_m), duration_s, json.dumps(geometry if geometry is not None else {})

    return addr_idx, None, None, None


def print_progress(completed, total, addr_idx, distance_m):
    now = time.monotonic()
    state = getattr(print_progress, "_state", None)
    if state is None:
        state = {"started_at": now, "last_print_at": 0.0}
        print_progress._state = state

    is_first = completed == 1
    is_done = completed >= total
    interval_elapsed = now - state["last_print_at"] >= ROUTING_PROGRESS_INTERVAL_S
    if not (is_first or is_done or interval_elapsed):
        return

    elapsed_s = max(now - state["started_at"], 0.001)
    tasks_per_s = completed / elapsed_s
    remaining = max(total - completed, 0)
    eta_s = remaining / tasks_per_s if tasks_per_s > 0 else None
    eta_text = "unbekannt" if eta_s is None else f"{eta_s:.0f} s" if eta_s < 60 else f"{eta_s / 60:.1f} min"
    dist_text = "keine Distanz" if distance_m is None else f"{distance_m:.1f} m"

    print(
        "[Routing-Fortschritt] "
        f"{completed}/{total} ({completed / total * 100:.1f} %) | "
        f"Adresse {addr_idx} -> Zentrum: {dist_text} | "
        f"{tasks_per_s:.2f} Tasks/s | ETA {eta_text} | "
        f"ORS ok/Fehler: {ROUTE_SUCCESS_COUNT}/{ROUTE_ERROR_COUNT}",
        flush=True,
    )
    state["last_print_at"] = now


def main():
    df_addr = pd.read_csv(CSV_ADDRESSES)
    df_addr = df_addr[df_addr["lat"].notna() & df_addr["lon"].notna()].copy()

    for col in ["Adresse_query", "display_name", "type", "category", "distance_m", "duration_s", "geojson"]:
        if col not in df_addr.columns:
            df_addr[col] = None

    df_addr["Adresse_query"] = df_addr.apply(
        lambda row: f"{row['Straßenname']}, {row['Hsnr']}{'' if pd.isna(row.get('HsnrZus')) else row.get('HsnrZus')}, brandenburg an der havel",
        axis=1,
    )
    df_addr["display_name"] = "Jahrtausendbruecke, Brandenburg an der Havel"
    df_addr["type"] = "center"
    df_addr["category"] = "center"

    tasks = [
        (idx, float(row["lon"]), float(row["lat"]))
        for idx, row in df_addr.iterrows()
    ]
    print("Anzahl Routing-Tasks:", len(tasks))
    print(f"Starte Zentrumsrouting ({MAX_WORKERS} Threads)...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(route_to_center, *task) for task in tasks]
        for completed, future in enumerate(as_completed(futures), start=1):
            addr_idx, distance_m, duration_s, geojson = future.result()
            df_addr.loc[addr_idx, "distance_m"] = distance_m
            df_addr.loc[addr_idx, "duration_s"] = duration_s
            df_addr.loc[addr_idx, "geojson"] = geojson
            print_progress(completed, len(futures), addr_idx, distance_m)

    print("Routing abgeschlossen.")
    print("ORS erfolgreiche Anfragen:", ROUTE_SUCCESS_COUNT)
    print("ORS Fehlanfragen:", ROUTE_ERROR_COUNT)
    print("Adressen mit Distanz:", int(df_addr["distance_m"].notna().sum()))

    output_cols = [
        "Straßenname",
        "Hsnr",
        "HsnrZus",
        "Adresse_query",
        "lat",
        "lon",
        "display_name",
        "type",
        "category",
        "distance_m",
        "duration_s",
        "geojson",
    ]
    passthrough_cols = [col for col in ["fid", "str-schluessel", "stand_der_daten", "geometry"] if col in df_addr.columns]
    df_addr[passthrough_cols + output_cols].to_csv(CSV_OUTPUT, index=False)
    print(f"Datei geschrieben: {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
