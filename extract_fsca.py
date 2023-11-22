import concurrent.futures
import os

import pandas as pd
import rasterio
from pyproj import Transformer
from rasterio.enums import Resampling

working_dir = os.getcwd()
folder_path = f"{working_dir}/final_output/"
station_cell_mapping = f"{working_dir}/station_cell_mapping.csv"


def get_value_at_coords(f_path, lat, lon, band_number=1):
  with rasterio.open(f_path) as src:
    transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
    east, north = transformer.transform(lon, lat)
    if not (src.bounds.left <= east <= src.bounds.right and src.bounds.bottom <= north <= src.bounds.top):
      return None
    row, col = src.index(east, north)
    if (0 <= row < src.height) and (0 <= col < src.width):
      return src.read(band_number, window=((row, row + 1), (col, col + 1)), resampling=Resampling.nearest)[0, 0]
    else:
      return None


def process_file(file_name):
  file_path = os.path.join(folder_path, file_name)
  data = []
  date = file_name.split('__')[0]  # Extract the date from the file name
  station_df = pd.read_csv(station_cell_mapping, low_memory=False, usecols=['lon', 'lat'])
  for idx, c in station_df.iterrows():
    lat = c['lat']
    lon = c['lon']
    band_value = get_value_at_coords(file_path, lat, lon, 1)
    if band_value is not None:
      data.append([date, lat, lon, band_value])
  file_name = os.path.splitext(file_name)[0]
  output_file = os.path.join(folder_path, f'{file_name}_output.csv')
  print(f"Processed file: {file_name}")
  pd.DataFrame(data, columns=['date', 'lat', 'lon', 'band_value']).to_csv(output_file, index=False)


def main():
  files_to_process = [file_name for file_name in os.listdir(folder_path) if file_name.endswith('.tif')]

  with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:  # Adjust max_workers as needed
    executor.map(process_file, files_to_process)


def merge_csv():
  import glob
  csv_files = glob.glob(folder_path + '*.csv')
  df = []
  for c in csv_files:
    tmp = pd.read_csv(c, low_memory=False, usecols=['date', 'lat', 'lon', 'band_value'])
    df.append(tmp)

  combined_df = pd.concat(df, ignore_index=True)
  combined_df.to_csv(f'{working_dir}/fsca_final.csv', index=False)


if __name__ == "__main__":
  # main()
  merge_csv()
  print("Data extraction complete.")
