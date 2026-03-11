import argparse

import geopandas as gpd
import pandas as pd
from shapely import wkt


def read_with_geometry(path: str) -> gpd.GeoDataFrame:
    df = pd.read_csv(path, encoding="utf-8")

    if "geometry" in df.columns:
        df["geometry"] = df["geometry"].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) and x.startswith("POINT") else None
        )
    elif {"lon", "lat"}.issubset(df.columns):
        df["geometry"] = gpd.points_from_xy(df["lon"], df["lat"])
    else:
        raise ValueError(f"{path}: benötigt 'geometry' oder ('lon','lat').")

    if {"lon", "lat"}.issubset(df.columns):
        lon_lat_mask = df["lon"].notna() & df["lat"].notna()
        df.loc[lon_lat_mask, "geometry"] = gpd.points_from_xy(
            df.loc[lon_lat_mask, "lon"], df.loc[lon_lat_mask, "lat"]
        )

    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def to_numeric_lines(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ergänzt in adressen_mit_haltestellen_routen.csv die Spalte "
            "'haltestellen_linien_count' per nearest Haltestellen-Matching."
        )
    )
    parser.add_argument(
        "--addresses",
        default="out/adressen_mit_haltestellen_routen.csv",
        help="Pfad zur Adress-CSV",
    )
    parser.add_argument(
        "--stops",
        default="out/haltestellen_geocoded.csv",
        help="Pfad zur Haltestellen-CSV",
    )
    parser.add_argument(
        "--output",
        default="out/adressen_mit_haltestellen_routen.csv",
        help="Pfad zur Ausgabe-CSV (Default: überschreibt addresses)",
    )
    args = parser.parse_args()

    gdf_addr = read_with_geometry(args.addresses)
    gdf_addr["__row_id"] = gdf_addr.index
    gdf_stops = read_with_geometry(args.stops)

    if "Anzahl der Linien" not in gdf_stops.columns:
        raise ValueError("Spalte 'Anzahl der Linien' fehlt in der Haltestellen-CSV.")

    gdf_stops = gdf_stops[["Anzahl der Linien", "geometry"]].copy()
    gdf_stops["haltestellen_linien_count"] = to_numeric_lines(gdf_stops["Anzahl der Linien"])

    gdf_addr_valid = gdf_addr[gdf_addr.geometry.notna()].copy()
    gdf_addr_m = gdf_addr_valid.to_crs("EPSG:25833")
    gdf_stops_m = gdf_stops[gdf_stops.geometry.notna()].to_crs("EPSG:25833")

    matched = gpd.sjoin_nearest(
        gdf_addr_m,
        gdf_stops_m[["haltestellen_linien_count", "geometry"]],
        how="left",
        distance_col="nearest_stop_distance_m",
    )

    # Bei exakt gleich nahen Haltestellen liefert sjoin_nearest mehrere Treffer.
    # Für stabile 1:1-Ergebnisse pro Adresszeile wird der "beste" Treffer gewählt.
    matched = matched.sort_values(
        ["__row_id", "nearest_stop_distance_m", "haltestellen_linien_count"],
        ascending=[True, True, False],
    ).drop_duplicates(subset="__row_id", keep="first")

    matched = matched.drop(columns=["index_right"], errors="ignore")
    linien_by_row = matched.set_index("__row_id")["haltestellen_linien_count"]

    out_df = pd.DataFrame(gdf_addr.drop(columns=["__row_id"], errors="ignore")).copy()
    out_df["haltestellen_linien_count"] = out_df.index.to_series().map(linien_by_row)
    out_df["geometry"] = gdf_addr.geometry.apply(lambda geom: geom.wkt if geom is not None else None)
    out_df = out_df.drop(columns=["nearest_stop_distance_m"], errors="ignore")
    out_df.to_csv(args.output, index=False, encoding="utf-8")

    missing = out_df["haltestellen_linien_count"].isna().sum()
    print(f"Geschrieben: {args.output}")
    print(f"Zeilen: {len(out_df)} | fehlende haltestellen_linien_count: {missing}")


if __name__ == "__main__":
    main()
