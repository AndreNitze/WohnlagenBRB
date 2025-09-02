import pandas as pd
import requests
import urllib.parse

# ------------------- Konfiguration -------------------
CSV_EINGABE = "data/2024_Haltestellen.csv"   # <---- anpassen!
CSV_AUSGABE = "data/haltestellen_geocoded.csv" # <---- anpassen!
CITY = "Brandenburg an der Havel"
COUNTRY = "Brandenburg"  # Additional indicator to narrow down geocoding results
ZIP_CODE = "14770"       # Additional indicator to narrow down geocoding results
#NOMINATIM_URL = "http://localhost:8081/search"

# ----- Online Service - Fair Use Policy beachten! ------
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
RATE_LIMIT = 1   # 0 für lokal, sonst 1
USER_AGENT = "Brandenburg University of Applied Sciences/1.0 (nitze@th-brandenburg.de)"

# ------------------- Hilfsfunktionen -------------------
def build_address(row):
    # --- Haltestellen ---
    if "Name_Haltestelle" in row and not pd.isna(row["Name_Haltestelle"]):
        name = str(row.get("Name_Haltestelle", "")).strip()

        # Strip name of commas
        name = name.replace(",", "").strip()

        kategorie = str(row.get("Kategorie", "")).strip().lower()
        if kategorie == "bus_stop":
            # Finde POI über englische Phrase, z.B. "Bus stop near Marzahne"
            return [f"Bus stop {name}, {COUNTRY}, {ZIP_CODE}"]
        elif kategorie == "tram_stop":
            return [f"Tram stop {name}, {COUNTRY}, {ZIP_CODE}"]
        else:
            return [f"{name}, {CITY}"]  # Fallback
    # --- Adressen ---
    street = row.get("Straßenname") or row.get("Straßennamen") or ""
    hn = str(row.get("Hsnr", "")).strip()
    hzusatz = str(row.get("HsnrZus", "")).strip() if "HsnrZus" in row and not pd.isna(row.get("HsnrZus")) else ""
    if hzusatz and hzusatz.lower() != "nan":
        hn += hzusatz
    parts = [street.strip(), hn, CITY]
    parts = [p for p in parts if p and p != "nan"]
    return [", ".join(parts)]

def make_merge_addr(row):
    # Erst Straße, dann Haltestelle
    street = row.get('Straßenname') or row.get('Straßennamen') or None
    hn = str(row.get('Hsnr', '')).strip().lower()
    hzusatz = str(row.get('HsnrZus', '')).strip().lower() if 'HsnrZus' in row and not pd.isna(row.get('HsnrZus', None)) else ""
    if hzusatz and hzusatz != "nan":
        hn += hzusatz
    if street:
        adr = f"{str(street).strip().lower()} {hn}".replace("  ", " ").strip()
    elif row.get("Name_Haltestelle"):
        # Für Haltestellen
        name = str(row.get("Name_Haltestelle")).strip().lower()
        if "haltestelle" not in name:
            name = f"haltestelle {name}"
        adr = f"{name} {hn}".replace("  ", " ").strip()
    else:
        adr = CITY.lower()
    return adr

def geocode_address(address_list, category=None):
    address = address_list[0]
    params = {
        'addressdetails': 1,
        'q': address,
        'format': 'jsonv2'
    }
    url = f"{NOMINATIM_URL}?{urllib.parse.urlencode(params)}"
    headers = {
        "User-Agent": USER_AGENT,
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    results = resp.json()
    if results:
        res = results[0]
        return {
            'lat': res.get('lat'),
            'lon': res.get('lon'),
            'display_name': res.get('display_name'),
            'type': res.get('type'),
            'category': res.get('category')
        }
    else:
        return {'lat': None, 'lon': None, 'display_name': None, 'type': None, 'category': None}

# ------------------- Hauptlogik -------------------
df = pd.read_csv(CSV_EINGABE, encoding="utf-8")
df["Adresse_query"] = df.apply(build_address, axis=1)
df["Adresse_merge"] = df.apply(make_merge_addr, axis=1)

for i, row in df.iterrows():
    address_list = build_address(row)
    category = None
    if "Kategorie" in row and not pd.isna(row["Kategorie"]):
        cat = str(row["Kategorie"]).lower()
        if cat in ("bus_stop", "tram_stop"):
            category = cat
    result = geocode_address(address_list, category=category)
    for key, value in result.items():
        df.loc[i, key] = value
    try:
        lat, lon = float(result['lat']), float(result['lon'])
        if pd.notna(lat) and pd.notna(lon):
            # GeoPandas-Standard: WKT
            geometry = f"POINT ({lon} {lat})"
        else:
            geometry = None
    except Exception:
        geometry = None
    df.loc[i, 'geometry'] = geometry
    print(f"[{i + 1}/{len(df)}] Geocoded: {address_list[0]} → {result['lat']}, {result['lon']} ({result['type']})")


# Ergebnis speichern
df.to_csv(CSV_AUSGABE, index=False, encoding="utf-8")
print(f"→ Geocoding abgeschlossen. Datei gespeichert als: {CSV_AUSGABE}")