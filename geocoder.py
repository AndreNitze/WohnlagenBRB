import csv
import time
import urllib.parse
from io import StringIO

import pandas as pd
import requests

# ------------------- Konfiguration -------------------
CSV_EINGABE = "data/2026_Haltestellen.csv"  # <---- anpassen!
CSV_AUSGABE = "out/haltestellen_geocoded.csv"  # <---- anpassen!
CITY = "Brandenburg an der Havel"
COUNTRY = "Brandenburg"  # Additional indicator to narrow down geocoding results
ZIP_CODE = "14770"  # Additional indicator to narrow down geocoding results

# ----- Online Service - Fair Use Policy beachten! ------
#NOMINATIM_URL = "http://localhost:8080/search"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# ---- Parameter fuer faire Nutzung ----
RATE_LIMIT = 1  # 0 fuer lokal, sonst 1
USER_AGENT = "Brandenburg University of Applied Sciences/1.0 (nitze@th-brandenburg.de)"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# ------------------- Hilfsfunktionen -------------------
def get_first_value(row, keys, default=""):
    for key in keys:
        if key in row and pd.notna(row[key]):
            value = row[key]
            if str(value).strip().lower() not in {"", "nan", "none"}:
                return value
    return default


def get_category(row):
    return str(get_first_value(row, ["Kategorie", "kategorie"], "")).strip().lower()


def normalize_input_dataframe(df):
    """Gleicht Korrektur-CSV-Spalten an das bisherige Haltestellen-Schema an."""
    rename_map = {
        "name": "Name_Haltestelle",
        "kategorie": "Kategorie",
        "Unnamed: 9": "Stadtlinien",
        "Anzahl der Stadtlinien (mit Nachtbus)": "Anzahl der Linien",
    }
    existing_renames = {
        source: target
        for source, target in rename_map.items()
        if source in df.columns and target not in df.columns
    }
    df = df.rename(columns=existing_renames)

    if "Anzahl der Linien" not in df.columns and "Anzahl der Stadtlinien (ohne Nachtbus)" in df.columns:
        df["Anzahl der Linien"] = df["Anzahl der Stadtlinien (ohne Nachtbus)"]

    return df


def get_street_name(row):
    # Unterstuetzt saubere und fehlerhaft kodierte Varianten.
    return str(
        get_first_value(
            row,
            [
                "Straßenname",
                "Straßennamen",
                "Strassenname",
                "Strassennamen",
                "StraÇYenname",
                "StraÇYennamen",
            ],
            "",
        )
    ).strip()


def build_address(row):
    # --- Sonderfall "Haltestellen" ---
    name = str(get_first_value(row, ["Name_Haltestelle", "name"], "")).strip()
    if name:
        name = name.replace(",", "").strip()
        kategorie = get_category(row)
        if kategorie == "bus_stop":
            return [
                f"Bus stop {name}, {COUNTRY}, {ZIP_CODE}",
                f"{name}, {CITY}",
            ]
        if kategorie == "tram_stop":
            return [
                f"Tram stop {name}, {COUNTRY}, {ZIP_CODE}",
                f"{name}, {CITY}",
            ]
        if kategorie == "train_stop":
            return [
                f"Train station {name}, {COUNTRY}, {ZIP_CODE}",
                f"{name}, {CITY}",
            ]
        return [f"{name}, {CITY}"]

    # --- Regulaere Adressen ---
    street = get_street_name(row)
    hn = str(get_first_value(row, ["Hsnr"], "")).strip()
    hzusatz = str(get_first_value(row, ["HsnrZus"], "")).strip()
    if hzusatz and hzusatz.lower() != "nan":
        hn += hzusatz

    parts = [street, hn, CITY]
    parts = [p for p in parts if p and p != "nan"]
    return [", ".join(parts)]


def load_input_csv(path):
    """Liest Haltestellen-/Adress-CSV robust mit Komma oder Semikolon ein."""
    df = pd.read_csv(path, encoding="utf-8-sig", sep=None, engine="python")

    # Fallback fuer Altdateien, bei denen die komplette CSV-Zeile in Spalte 1 steckt.
    if len(df.columns) == 1:
        first_col = df.columns[0]
        parsed_header = next(csv.reader(StringIO(first_col)))
        if len(parsed_header) > 1:
            parsed_rows = []
            for value in df.iloc[:, 0].fillna("").astype(str):
                parsed = next(csv.reader(StringIO(value)))
                if len(parsed) > len(parsed_header):
                    overflow = len(parsed) - len(parsed_header)
                    parsed = [",".join(parsed[:overflow + 1])] + parsed[overflow + 1:]
                if len(parsed) == len(parsed_header):
                    parsed_rows.append(parsed)

            if parsed_rows:
                df = pd.DataFrame(parsed_rows, columns=parsed_header)

    return df


def make_merge_addr(row):
    # Erst Strasse, dann Haltestelle
    street = get_street_name(row)
    hn = str(get_first_value(row, ["Hsnr"], "")).strip().lower()
    hzusatz = str(get_first_value(row, ["HsnrZus"], "")).strip().lower()
    if hzusatz and hzusatz != "nan":
        hn += hzusatz

    if street:
        return f"{street.lower()} {hn}".replace("  ", " ").strip()

    name = str(get_first_value(row, ["Name_Haltestelle", "name"], "")).strip().lower()
    if name:
        if "haltestelle" not in name:
            name = f"haltestelle {name}"
        return f"{name} {hn}".replace("  ", " ").strip()

    return CITY.lower()


def geocode_address(address_list):
    headers = {"User-Agent": USER_AGENT}

    for address in address_list:
        if not address:
            continue

        params = {
            "addressdetails": 1,
            "q": address,
            "format": "jsonv2",
        }
        url = f"{NOMINATIM_URL}?{urllib.parse.urlencode(params)}"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                resp.raise_for_status()
                results = resp.json()
                if results:
                    res = results[0]
                    return {
                        "lat": res.get("lat"),
                        "lon": res.get("lon"),
                        "display_name": res.get("display_name"),
                        "type": res.get("type"),
                        "category": res.get("category"),
                    }
                break
            except requests.RequestException as exc:
                if attempt >= MAX_RETRIES:
                    print(f"Geocoding fehlgeschlagen fuer '{address}': {exc}")
                else:
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            finally:
                if RATE_LIMIT > 0:
                    time.sleep(RATE_LIMIT)

    return {
        "lat": None,
        "lon": None,
        "display_name": None,
        "type": None,
        "category": None,
    }


def main():
    df = normalize_input_dataframe(load_input_csv(CSV_EINGABE))
    df["Adresse_query"] = df.apply(build_address, axis=1)
    df["Adresse_merge"] = df.apply(make_merge_addr, axis=1)

    for i, row in df.iterrows():
        address_list = row["Adresse_query"]
        if not isinstance(address_list, list):
            address_list = [str(address_list)]

        result = geocode_address(address_list)

        for key, value in result.items():
            df.loc[i, key] = value

        try:
            lat = float(result["lat"])
            lon = float(result["lon"])
            if pd.notna(lat) and pd.notna(lon):
                # GeoPandas-Standard: WKT
                geometry = f"POINT ({lon} {lat})"
            else:
                geometry = None
        except (TypeError, ValueError):
            geometry = None

        df.loc[i, "geometry"] = geometry
        print(f"[{i + 1}/{len(df)}] Geocoded: {address_list[0]} -> {result['lat']}, {result['lon']} ({result['type']})")

    df.to_csv(CSV_AUSGABE, index=False, encoding="utf-8")
    print(f"Geocoding abgeschlossen. Datei gespeichert als: {CSV_AUSGABE}")


if __name__ == "__main__":
    main()
