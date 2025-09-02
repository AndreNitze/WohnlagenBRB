import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point

def load_geocsv(path, crs="EPSG:4326", geometry_col="geometry"):
    df = pd.read_csv(path, encoding="utf-8")

    # FALL 1: "geometry" existiert
    if geometry_col in df.columns:
        # Parse vorhandene Werte; falls leer, bleibt es None
        df[geometry_col] = df[geometry_col].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) and x.startswith("POINT") else None
        )
    # FALL 2: "geometry" existiert NICHT
    else:
        if "lon" in df.columns and "lat" in df.columns:
            # Erzeuge komplett neue "geometry"-Spalte
            df[geometry_col] = df.apply(
                lambda row: Point(float(row["lon"]), float(row["lat"]))
                if pd.notna(row["lon"]) and pd.notna(row["lat"]) else None,
                axis=1
            )
        else:
            raise ValueError(
                "Fehlt sowohl 'geometry' als auch ('lat', 'lon')! "
                f"Gefunden: {df.columns.tolist()}"
            )
    # Mach GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry_col, crs=crs)
    return gdf

def geo_sjoin(left, right, how="left", predicate="intersects", drop_index_cols=True, suffixes=("", "_joined")):
    result = gpd.sjoin(left, right, how=how, predicate=predicate, lsuffix=suffixes[0], rsuffix=suffixes[1])
    if drop_index_cols:
        result = result[[col for col in result.columns if not col.startswith("index_")]]
    return result