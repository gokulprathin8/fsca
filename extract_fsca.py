import rasterio
import pandas as pd
from pyproj import transform, CRS


def get_value_at_coords(file_path, lat, lon, band_number=1):
  with rasterio.open(file_path) as src:
    src_crs = CRS.from_user_input(src.crs)
    lat_lon_crs = CRS.from_user_input('EPSG:4326')
    east, north = transform(lat_lon_crs, src_crs, lat, lon)
    row, col = src.index(east, north)
    return src.read(band_number)[row, col]

station_df = pd.read_csv('station_cell_mapping.csv', low_memory=False, usecols=['lon', 'lat'])
for idx, c in station_df.iterrows():
  lat = c['lat']
  lon = c['lon']
  band_value = get_value_at_coords('/home/gokul/fsca/2018-01-01__snow_cover.tif', lat, lon, 1)
  print(band_value)
