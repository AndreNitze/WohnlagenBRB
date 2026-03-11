import pandas as pd
from pyproj import Transformer

INPUT_CSV = "out/haltestellen_geocoded.csv"
OUTPUT_CSV = "out/haltestellen_geocoded.csv"
X_COL = "x"
Y_COL = "y"

df = pd.read_csv(INPUT_CSV, sep=",", dtype=str)

df[X_COL] = df[X_COL].str.replace(",", ".", regex=False).astype(float)
df[Y_COL] = df[Y_COL].str.replace(",", ".", regex=False).astype(float)

transformer = Transformer.from_crs("EPSG:25833", "EPSG:4326", always_xy=True)

lon, lat = transformer.transform(df[X_COL].values, df[Y_COL].values)

if "lon" not in df.columns:
    df["lon"] = pd.NA
if "lat" not in df.columns:
    df["lat"] = pd.NA

df["lon"] = pd.to_numeric(df["lon"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
df["lat"] = pd.to_numeric(df["lat"].astype(str).str.replace(",", ".", regex=False), errors="coerce")

missing_coords_mask = df["lon"].isna() & df["lat"].isna()
df.loc[missing_coords_mask, "lon"] = lon[missing_coords_mask]
df.loc[missing_coords_mask, "lat"] = lat[missing_coords_mask]
df["geometry"] = [f"POINT({x} {y})" for x, y in zip(df["lon"], df["lat"])]

df.to_csv(OUTPUT_CSV, sep=",", index=False)
print(df.head())
