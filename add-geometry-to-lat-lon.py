# read csv with adresses
import pandas as pd

df = pd.read_csv("out/adressen_geocoded.csv", encoding="utf-8", sep=",")

# Convert lat/lon columns to Geometry
from shapely.geometry import Point
df['geometry'] = df.apply(lambda row: Point(float(row['lon']), float(row['lat'])) if pd.notna(row['lat']) and pd.notna(row['lon']) else None, axis=1)

# Save to a new CSV file with geometry
df.to_csv("data/adressen_geocoded.csv", encoding="utf-8", index=False)
