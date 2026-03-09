import pandas as pd
from pyproj import Transformer

INPUT_CSV = "data/2026_Haltestellen.csv"
OUTPUT_CSV = "out/haltestellen_geocoded.csv"
X_COL = "x"
Y_COL = "y"

df = pd.read_csv(INPUT_CSV, sep=",", dtype=str)

df[X_COL] = df[X_COL].str.replace(",", ".", regex=False).astype(float)
df[Y_COL] = df[Y_COL].str.replace(",", ".", regex=False).astype(float)

transformer = Transformer.from_crs("EPSG:25833", "EPSG:4326", always_xy=True)

lon, lat = transformer.transform(df[X_COL].values, df[Y_COL].values)

df["lon"] = lon
df["lat"] = lat
df["geometry"] = [f"POINT({x} {y})" for x, y in zip(lon, lat)]

df.to_csv(OUTPUT_CSV, sep=",", index=False)
print(df.head())