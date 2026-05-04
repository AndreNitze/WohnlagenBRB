import pandas as pd
import numpy as np
import requests
import json
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points

# ---------------------------------------------------------
# MODUS-KONFIGURATION
# ---------------------------------------------------------
ROUTING_MODE = "poi"     # Routing zu Punktzielen
#ROUTING_MODE = "area"      # Routing zu Flaechen
# ---------------------------------------------------------
# ORS KONFIGURATION
# ---------------------------------------------------------
ORS_URL     = "http://localhost:8080/ors/v2/directions/foot-walking/geojson"
ORS_API_KEY = "your_api_key_here"

# ---------------------------------------------------------
# DATEIEN
# ---------------------------------------------------------
DOMAIN            = "einzelhandel"
CSV_ADDRESSES     = "out/adressen_geocoded.csv"
CSV_DESTINATIONS  = "out/" + DOMAIN + "_geocoded.csv" # Wenn POI-Modus
AREA_PATH         = "data/Grünflächen_Verkehrszeichen/20251029_Vegetation_KSP_GP_31.shp" # Wenn AREA-Modus
CSV_OUTPUT        = "out/adressen_mit_" + DOMAIN + "_routen.csv"

# ---------------------------------------------------------
# PARAMETER
# ---------------------------------------------------------
DISTANCE_THRESHOLDS = [500]     # Zaehlradius
POI_MAX_CANDIDATES  = 50                   # bis hierhin werden POI-Domaenen komplett geroutet
POI_ADAPTIVE_BATCH_SIZE = 10                  # Routenblock fuer adaptive POI-Suche
MAX_WORKERS         = 8                   # parallele Threads
ROUTING_ENABLED     = True                 # debug/skip moeglich
ORS_TIMEOUT_S       = 15                   # request timeout
ORS_RETRY_RADII_M   = [None, 1200, 2500]   # Retry bei ORS 2010 (Snapping)
ROUTING_PROGRESS_INTERVAL_S = 5           # Fortschrittsausgabe alle n Sekunden

# ---------------------------------------------------------
# DEBUG-COUNTER
# ---------------------------------------------------------
ROUTE_SUCCESS_COUNT = 0
ROUTE_ERROR_COUNT   = 0
ROUTE_ERROR_LIMIT   = 10    # max. 10 Fehler detailliert ausgeben
COUNTER_LOCK = threading.Lock()

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


def haversine_single(lat1, lon1, lat2, lon2):
    """Skalar-Haversine-Distanz (Meter)."""
    return float(haversine_np(lat1, lon1, lat2, lon2))


def distance_from_linestring_m(geometry):
    """Fallback: Distanz in Metern aus einer GeoJSON-LineString-Geometrie."""
    if not isinstance(geometry, dict):
        return None
    if geometry.get("type") != "LineString":
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


def route_distance(lon_a, lat_a, lon_b, lat_b):
    """ORS Fussrouting: gibt (dist_m, Liniengeometrie_json) zurueck."""
    global ROUTE_SUCCESS_COUNT, ROUTE_ERROR_COUNT

    # Guard: falls Koordinaten None/NaN sind, Routing ueberspringen
    if lon_a is None or lat_a is None or lon_b is None or lat_b is None:
        return None, None
    if any(np.isnan([lon_a, lat_a, lon_b, lat_b])):
        return None, None

    if not ROUTING_ENABLED:
        # Distanzberechnung kann zum Debuggen abgeschaltet werden
        return None, None

    # identische Punkte direkt behandeln, um leere ORS-Summaries zu vermeiden
    if np.isclose(lon_a, lon_b, atol=1e-8) and np.isclose(lat_a, lat_b, atol=1e-8):
        geom = {"type": "LineString", "coordinates": [[float(lon_a), float(lat_a)]]}
        with COUNTER_LOCK:
            ROUTE_SUCCESS_COUNT += 1
            success_count = ROUTE_SUCCESS_COUNT
        if success_count <= 5:
            print("[ORS-OK] Beispiel-Distanz: 0.0 m (identische Punkte)")
        return 0.0, json.dumps(geom)

    try:
        for radius in ORS_RETRY_RADII_M:
            payload = {
                "coordinates": [[lon_a, lat_a], [lon_b, lat_b]],
                "instructions": False,
                "geometry": True,
                "preference": "recommended",
                "options": {
                    "avoid_features": ["ferries"]
                }
            }
            if radius is not None:
                payload["radiuses"] = [radius, radius]

            r = requests.post(
                ORS_URL,
                data=json.dumps(payload),
                headers={
                    "Authorization": ORS_API_KEY,
                    "Content-Type": "application/json"
                },
                timeout=ORS_TIMEOUT_S
            )

            if r.status_code != 200:
                # bei ORS-2010 mit groesserem Radius erneut versuchen
                if r.status_code == 404:
                    try:
                        err_data = r.json()
                        err_code = err_data.get("error", {}).get("code")
                    except Exception:
                        err_code = None
                    if err_code == 2010 and radius is not ORS_RETRY_RADII_M[-1]:
                        continue

                with COUNTER_LOCK:
                    ROUTE_ERROR_COUNT += 1
                    error_count = ROUTE_ERROR_COUNT
                if error_count <= ROUTE_ERROR_LIMIT:
                    print(f"[ORS-ERROR] Status: {r.status_code}")
                    # nur die ersten ~300 Zeichen, damit Console nicht explodiert
                    print(f"[ORS-ERROR] Antwort: {r.text[:300]}")
                    print(f"[ORS-ERROR] Payload: {payload}")
                return None, None

            data = r.json()
            if "features" not in data or not data["features"]:
                with COUNTER_LOCK:
                    ROUTE_ERROR_COUNT += 1
                    error_count = ROUTE_ERROR_COUNT
                if error_count <= ROUTE_ERROR_LIMIT:
                    print("[ORS-ERROR] Keine 'features' im Response.")
                    print(f"[ORS-ERROR] Antwort: {str(data)[:300]}")
                    print(f"[ORS-ERROR] Payload: {payload}")
                return None, None

            feat = data["features"][0]
            properties = feat.get("properties", {}) if isinstance(feat, dict) else {}
            summary = properties.get("summary", {}) if isinstance(properties, dict) else {}
            geometry = feat.get("geometry") if isinstance(feat, dict) else None

            dist = summary.get("distance") if isinstance(summary, dict) else None
            if dist is None:
                dist = distance_from_linestring_m(geometry)

            if dist is None:
                with COUNTER_LOCK:
                    ROUTE_ERROR_COUNT += 1
                    error_count = ROUTE_ERROR_COUNT
                if error_count <= ROUTE_ERROR_LIMIT:
                    print("[ORS-ERROR] Distanz fehlt im Response und konnte nicht abgeleitet werden.")
                    print(f"[ORS-ERROR] Antwort: {str(data)[:300]}")
                    print(f"[ORS-ERROR] Payload: {payload}")
                return None, None

            geom = json.dumps(geometry if geometry is not None else {})

            with COUNTER_LOCK:
                ROUTE_SUCCESS_COUNT += 1
                success_count = ROUTE_SUCCESS_COUNT
            if success_count <= 5:
                print(f"[ORS-OK] Beispiel-Distanz: {dist} m")

            return float(dist), geom

    except Exception as e:
        with COUNTER_LOCK:
            ROUTE_ERROR_COUNT += 1
            error_count = ROUTE_ERROR_COUNT
        if error_count <= ROUTE_ERROR_LIMIT:
            print(f"[ORS-EXCEPTION] {type(e).__name__}: {e}")
            print(f"[ORS-EXCEPTION] Payload: {payload}")
        return None, None


def nearest_entry_point(point_wgs84, area_geom, centroid_wgs=None):
    """
    Berechnet robust einen Eintrittspunkt fuer EINE Gruenflaeche (Polygon oder MultiPolygon).
    Gibt (lon, lat) in WGS84 zurueck oder (None, None), falls nicht moeglich.
    """
    try:
        # Punkt -> Meter (UTM Zone 33N)
        point_m = gpd.GeoSeries([point_wgs84], crs=4326).to_crs(32633).iloc[0]
    except Exception:
        if centroid_wgs is not None:
            return float(centroid_wgs.x), float(centroid_wgs.y)
        return None, None

    if area_geom is None or area_geom.empty:
        if centroid_wgs is not None:
            return float(centroid_wgs.x), float(centroid_wgs.y)
        return None, None

    geom = area_geom.geometry.iloc[0]
    if geom is None or geom.is_empty:
        if centroid_wgs is not None:
            return float(centroid_wgs.x), float(centroid_wgs.y)
        return None, None

    # Multipolygon abfangen: groesstes Polygon waehlen
    if geom.geom_type == "MultiPolygon":
        if len(geom.geoms) == 0:
            if centroid_wgs is not None:
                return float(centroid_wgs.x), float(centroid_wgs.y)
            return None, None
        geom = max(geom.geoms, key=lambda g: g.area)

    # ungueltige Geometrien reparieren
    if not geom.is_valid:
        geom = geom.buffer(0)
        if geom.is_empty:
            if centroid_wgs is not None:
                return float(centroid_wgs.x), float(centroid_wgs.y)
            return None, None

    # boundary extrahieren
    boundary = geom.boundary
    if boundary.is_empty:
        # Fallback: naechster Punkt auf Flaeche statt auf Boundary
        try:
            nearest = nearest_points(point_m, geom)[1]
            entry_wgs = gpd.GeoSeries([nearest], crs=32633).to_crs(4326).iloc[0]
            lon = float(entry_wgs.x)
            lat = float(entry_wgs.y)
            if np.isnan(lon) or np.isnan(lat):
                if centroid_wgs is not None:
                    return float(centroid_wgs.x), float(centroid_wgs.y)
                return None, None
            return lon, lat
        except Exception:
            if centroid_wgs is not None:
                return float(centroid_wgs.x), float(centroid_wgs.y)
            return None, None

    try:
        proj = boundary.project(point_m)
        entry_m = boundary.interpolate(proj)

        if entry_m.is_empty:
            if centroid_wgs is not None:
                return float(centroid_wgs.x), float(centroid_wgs.y)
            return None, None

        entry_wgs = gpd.GeoSeries([entry_m], crs=32633).to_crs(4326).iloc[0]
        lon = float(entry_wgs.x)
        lat = float(entry_wgs.y)
        if np.isnan(lon) or np.isnan(lat):
            if centroid_wgs is not None:
                return float(centroid_wgs.x), float(centroid_wgs.y)
            return None, None
        return lon, lat
    except Exception:
        if centroid_wgs is not None:
            return float(centroid_wgs.x), float(centroid_wgs.y)
        return None, None


def route_task(args):
    """ThreadPool-Routing."""
    lon_a, lat_a, lon_d, lat_d, addr_idx, area_id = args
    dist, geom = route_distance(lon_a, lat_a, lon_d, lat_d)
    return addr_idx, area_id, dist, geom


def route_poi_address_task(args):
    """
    Routet POI-Ziele fuer eine Adresse.

    Bei kleinen POI-Domaenen werden alle Ziele geroutet. Bei groesseren
    Domaenen wird nach Luftlinie sortiert und in Bloecken geroutet. Sobald
    die naechste ungepruefte Luftlinie groesser/gleich der besten gefundenen
    Routingdistanz ist, kann kein ungeprueftes Ziel mehr gewinnen.
    """
    addr_idx, lon_a, lat_a = args
    dists = haversine_np(lat_a, lon_a, lats_dest, lons_dest)
    candidate_pos = np.argsort(dists)

    dist_map = {}
    best_dist = None
    last_area_id = None
    last_dist = None

    for start in range(0, len(candidate_pos), POI_ADAPTIVE_BATCH_SIZE):
        batch = candidate_pos[start:start + POI_ADAPTIVE_BATCH_SIZE]

        for pos in batch:
            lat_d = float(lats_dest[pos])
            lon_d = float(lons_dest[pos])
            poi_id = f"poi_{int(dest_ids[pos])}"
            dist, geom = route_distance(lon_a, lat_a, lon_d, lat_d)
            dist_map[poi_id] = (dist, geom)
            last_area_id = poi_id
            last_dist = dist
            if dist is not None and (best_dist is None or dist < best_dist):
                best_dist = dist

        next_pos_idx = start + POI_ADAPTIVE_BATCH_SIZE
        if len(candidate_pos) > POI_MAX_CANDIDATES and best_dist is not None:
            if next_pos_idx >= len(candidate_pos):
                break
            next_air_dist = float(dists[candidate_pos[next_pos_idx]])
            count_threshold = max(DISTANCE_THRESHOLDS) if DISTANCE_THRESHOLDS else 0
            if next_air_dist >= max(best_dist, count_threshold):
                break

    return addr_idx, dist_map, last_area_id, last_dist


def format_target_name(dest_row, fallback_col=None):
    """Erzeugt lesbare POI-Namen, bei Maerkten inklusive Betreiber."""
    parts = []
    for col in ["Name_Unternehmen", "Name_Haltestelle", "name", "Name"]:
        if col in dest_row.index and pd.notna(dest_row[col]) and str(dest_row[col]).strip():
            parts.append(str(dest_row[col]).strip())
            break

    address_parts = []
    if "Straßenname" in dest_row.index and pd.notna(dest_row["Straßenname"]):
        address_parts.append(str(dest_row["Straßenname"]).strip())
    if "Hsnr" in dest_row.index and pd.notna(dest_row["Hsnr"]):
        address_parts.append(str(dest_row["Hsnr"]).strip())
    if "HsnrZus" in dest_row.index and pd.notna(dest_row["HsnrZus"]):
        address_parts.append(str(dest_row["HsnrZus"]).strip())
    if address_parts:
        address = " ".join(address_parts)
        if not parts or address.casefold() not in parts[0].casefold():
            parts.append(address)

    if not parts and fallback_col is not None and fallback_col in dest_row.index:
        value = dest_row[fallback_col]
        if pd.notna(value) and str(value).strip():
            parts.append(str(value).strip())

    return ", ".join(parts) if parts else None


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

    if df_dest.empty:
        raise ValueError(
            f"POI-Modus aktiv, aber keine gueltigen Ziele (lat/lon) in Datei: {CSV_DESTINATIONS}"
        )

    lats_dest = df_dest["lat"].to_numpy()
    lons_dest = df_dest["lon"].to_numpy()
    dest_ids  = df_dest.index.to_numpy()

    dest_name_col = None
    for candidate in ["Name_Haltestelle", "name", "Name", "Adresse_merge"]:
        if candidate in df_dest.columns:
            dest_name_col = candidate
            break

else:
    df_dest = None
    dest_name_col = None


# ---------------------------------------------------------
# DATEN LADEN: GRUENFLAECHEN (nur AREA-Modus)
# ---------------------------------------------------------
if ROUTING_MODE == "area":

    print("Lade Gruenflaechen...")

    if not os.path.exists(AREA_PATH):
        raise FileNotFoundError(f"AREA_PATH nicht gefunden: {AREA_PATH}")

    gdf_gruen = gpd.read_file(AREA_PATH)

    # Dissolve nach objektbeze
    gdf_area = gdf_gruen.to_crs(32633).dissolve(by="objektbeze").reset_index()

    if gdf_area.empty:
        raise ValueError(
            f"AREA-Modus aktiv, aber keine Flaechen nach dissolve in: {AREA_PATH}"
        )

    # Debug Geometrien
    print("Pruefe Geometrien nach dissolve()...")
    print("Anzahl ungueltiger Geometrien:", sum(~gdf_area.geometry.is_valid))
    print("Anzahl leerer Geometrien:", sum(gdf_area.geometry.is_empty))
    print("Geom-Typ-Uebersicht:")
    print(gdf_area.geometry.geom_type.value_counts())

    # Centroid fuer Kandidatensuche
    gdf_area["centroid_wgs"] = gdf_area.geometry.centroid.to_crs(4326)

    area_ids      = gdf_area["objektbeze"].tolist()
    area_cent_lat = gdf_area["centroid_wgs"].y.to_numpy()
    area_cent_lon = gdf_area["centroid_wgs"].x.to_numpy()

    print(f"{len(gdf_area)} Gruenflaechen-Gruppen geladen.")

else:
    gdf_area = None


# ---------------------------------------------------------
# ROUTING-TASKS ERZEUGEN
# ---------------------------------------------------------
tasks = []
print("Erzeuge Routing-Tasks...")

for addr_idx, row in df_addr.iterrows():

    lat_a = float(row["lat"])
    lon_a = float(row["lon"])
    pt    = Point(lon_a, lat_a)

    # -----------------------------------------------------
    # POI-MODUS
    # -----------------------------------------------------
    if ROUTING_MODE == "poi":
        tasks.append((addr_idx, lon_a, lat_a))

    # -----------------------------------------------------
    # AREA-MODUS - mehrere GRUENFLAECHEN pro Adresse
    # -----------------------------------------------------
    elif ROUTING_MODE == "area":

        # Schritt 1: Luftlinien-Kandidatensuche
        dists = haversine_np(lat_a, lon_a, area_cent_lat, area_cent_lon)

        cand_mask = dists <= 1500
        if not cand_mask.any():
            nearest_idx = int(np.argmin(dists))
            cand_mask[nearest_idx] = True
        cand_areas = gdf_area[cand_mask]

        # Schritt 2: Fuer jede Kandidaten-Gruenflaeche -> Entry Point + Task
        for _, area_row in cand_areas.iterrows():

            area_id = area_row["objektbeze"]
            area_geom = gdf_area[gdf_area["objektbeze"] == area_id]
            centroid_wgs = area_row["centroid_wgs"] if "centroid_wgs" in area_row else None

            lon_d, lat_d = nearest_entry_point(pt, area_geom, centroid_wgs)

            if lon_d is None or lat_d is None:
                # Nur fuer Diagnose:
                # print(f"[WARN] Kein Entry fuer Flaeche {area_id} bei Adresse {addr_idx}.")
                continue

            tasks.append((lon_a, lat_a, lon_d, lat_d, addr_idx, area_id))

print("Anzahl Routing-Tasks:", len(tasks))

# ---------------------------------------------------------
# PARALLELES ROUTING
# ---------------------------------------------------------
print(f"Starte paralleles Routing ({MAX_WORKERS} Threads)...")
results = defaultdict(dict)


def print_routing_progress(completed_futures, total_futures, addr_idx, area_id, dist):
    """Gibt den Routing-Fortschritt als fortlaufende Logzeilen aus."""
    if total_futures <= 0:
        return

    now = time.monotonic()
    state = getattr(print_routing_progress, "_state", None)
    if state is None or completed_futures <= 1:
        state = {
            "started_at": now,
            "last_print_at": 0.0,
        }
        print_routing_progress._state = state

    is_first = completed_futures == 1
    is_done = completed_futures >= total_futures
    interval_elapsed = now - state["last_print_at"] >= ROUTING_PROGRESS_INTERVAL_S
    if not (is_first or is_done or interval_elapsed):
        return

    elapsed_s = max(now - state["started_at"], 0.001)
    progress_pct = completed_futures / total_futures * 100
    tasks_per_s = completed_futures / elapsed_s
    remaining = max(total_futures - completed_futures, 0)
    eta_s = remaining / tasks_per_s if tasks_per_s > 0 else None

    if eta_s is None:
        eta_text = "unbekannt"
    elif eta_s < 60:
        eta_text = f"{eta_s:.0f} s"
    else:
        eta_text = f"{eta_s / 60:.1f} min"

    dist_text = "keine Distanz" if dist is None else f"{dist:.1f} m"
    print(
        "[Routing-Fortschritt] "
        f"{completed_futures}/{total_futures} ({progress_pct:.1f} %) | "
        f"Adresse {addr_idx} -> {area_id}: {dist_text} | "
        f"{tasks_per_s:.2f} Tasks/s | ETA {eta_text} | "
        f"ORS ok/Fehler: {ROUTE_SUCCESS_COUNT}/{ROUTE_ERROR_COUNT}",
        flush=True,
    )
    state["last_print_at"] = now



if ROUTING_ENABLED and len(tasks) > 0:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        task_func = route_poi_address_task if ROUTING_MODE == "poi" else route_task
        future_list = [ex.submit(task_func, t) for t in tasks]
        total_futures = len(future_list)
        completed_futures = 0

        for fut in as_completed(future_list):
            if ROUTING_MODE == "poi":
                addr_idx, dist_map, area_id, dist = fut.result()
                results[addr_idx].update(dist_map)
            else:
                addr_idx, area_id, dist, geom = fut.result()
                # dist kann None sein, wir speichern trotzdem
                results[addr_idx][area_id] = (dist, geom)
            completed_futures += 1
            print_routing_progress(
                completed_futures,
                total_futures,
                addr_idx,
                area_id,
                dist,
            )
    print()

print("Routing abgeschlossen.")
print("ORS erfolgreiche Anfragen:", ROUTE_SUCCESS_COUNT)
print("ORS Fehlanfragen:", ROUTE_ERROR_COUNT)
print("Anzahl Adressen im results-Objekt:", len(results))
print(
    "Adressen mit mindestens einer Distanz (dist != None):",
    sum(
        any(dist_geom[0] is not None for dist_geom in dist_map.values())
        for dist_map in results.values()
    )
)

# ---------------------------------------------------------
# ERGEBNISSE ZUSAMMENFUEHREN
# ---------------------------------------------------------
df_addr[DOMAIN + "_min_distance"] = None
df_addr[DOMAIN + "_route"] = None
df_addr[DOMAIN + "_target_id"] = None
df_addr[DOMAIN + "_target_name"] = None
df_addr[DOMAIN + "_target_lat"] = None
df_addr[DOMAIN + "_target_lon"] = None

for d in DISTANCE_THRESHOLDS:
    df_addr[f"{DOMAIN}_count_within_{d}m"] = 0

for addr_idx in df_addr.index:
    dist_map = results.get(addr_idx, {})
    if len(dist_map) == 0:
        continue

    # Fuer Statistik: nur Distanzen extrahieren
    distances_only = [
        dist_geom[0]
        for dist_geom in dist_map.values()
        if dist_geom[0] is not None
    ]

    if len(distances_only) == 0:
        continue

    # Fuer beste Route: komplette Eintraege
    valid_entries = [
        (area_id, dist_geom[0], dist_geom[1])
        for area_id, dist_geom in dist_map.items()
        if dist_geom[0] is not None
    ]

    if len(valid_entries) > 0:
        # Minimale echte Routingdistanz finden.
        best_area, best_dist, best_geom = min(valid_entries, key=lambda x: x[1])

        # Eintragen
        df_addr.loc[addr_idx, DOMAIN + "_min_distance"] = best_dist
        df_addr.loc[addr_idx, DOMAIN + "_route"] = best_geom

    if ROUTING_MODE == "poi" and str(best_area).startswith("poi_"):
        dest_idx = int(str(best_area).split("_", 1)[1])
        df_addr.loc[addr_idx, DOMAIN + "_target_id"] = dest_idx
        df_addr.loc[addr_idx, DOMAIN + "_target_lat"] = df_dest.loc[dest_idx, "lat"]
        df_addr.loc[addr_idx, DOMAIN + "_target_lon"] = df_dest.loc[dest_idx, "lon"]
        target_name = format_target_name(df_dest.loc[dest_idx], dest_name_col)
        if target_name is not None:
            df_addr.loc[addr_idx, DOMAIN + "_target_name"] = target_name
    else:
        df_addr.loc[addr_idx, DOMAIN + "_target_id"] = best_area

    # Count innerhalb thresholds
    for d in DISTANCE_THRESHOLDS:
        df_addr.loc[addr_idx, f"{DOMAIN}_count_within_{d}m"] = sum(
            dist <= d for dist in distances_only
        )


# ---------------------------------------------------------
# SPEICHERN
# ---------------------------------------------------------
df_addr.to_csv(CSV_OUTPUT, index=False)
print(f"Datei geschrieben: {CSV_OUTPUT}")
