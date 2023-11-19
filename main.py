import os
import subprocess
import threading
from datetime import datetime
from datetime import timedelta

import requests
import earthaccess
from osgeo import gdal

start_date = datetime(2019, 1, 1)
end_date = datetime(2019, 1, 3)
tile_list = ["h08v04", "h08v05", "h09v04", "h09v05", "h10v04", "h10v05", "h11v04", "h11v05", "h12v04", "h12v05",
             "h13v04", "h13v05", "h15v04", "h16v03", "h16v04", ]
input_folder = os.getcwd() + "/temp/"
output_folder = os.getcwd() + "/output_folder/"
os.makedirs(output_folder, exist_ok=True)

def convert_hdf_to_geotiff(hdf_file, output_folder):
  # Open the HDF file
  hdf_ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)

  # Loop through each subdataset in the HDF file
  for subdataset in hdf_ds.GetSubDatasets():
    # Extract the name of the subdataset
    name = subdataset[0].split(':')[-1]

    # Open the subdataset
    ds = gdal.Open(subdataset[0], gdal.GA_ReadOnly)

    # Define the output path
    output_path = os.path.join(output_folder, f"{name}.tif")

    # Convert to GeoTIFF
    gdal.Translate(output_path, ds)

    # Close the subdataset
    ds = None

  # Close the HDF dataset
  hdf_ds = None


def convert_all_hdf_in_folder(folder_path, output_folder):
  # List all files in the given folder
  for file in os.listdir(folder_path):
    # Check if the file is an HDF file
    if file.lower().endswith(".hdf"):
      hdf_file = os.path.join(folder_path, file)
      convert_hdf_to_geotiff(hdf_file, output_folder)
      print(f"Converted {file} to GeoTIFF")


def list_files(directory):
  return [os.path.abspath(os.path.join(directory, f)) for f in os.listdir(directory) if
          os.path.isfile(os.path.join(directory, f))]


def merge_tiles(date, hdf_files):
  path = f"data/{date}/"
  files = list_files(path)
  print(files)
  merged_filename = f"data/{date}/merged.tif"
  merge_command = ["gdal_merge.py", "-o", merged_filename, "-of", "GTiff"] + files
  try:
    subprocess.run(merge_command)
    print(f"Merged tiles into {merged_filename}")
  except subprocess.CalledProcessError as e:
    print(f"Error merging tiles: {e}")


def download_url(date, url):
  file_name = url.split('/')[-1]
  if os.path.exists(f'data/{date}/{file_name}'):
    print(f'File: {file_name} already exists, SKIPPING')
    return
  try:
    os.makedirs('data/', exist_ok=True)
    os.makedirs(f'data/{date}', exist_ok=True)
    response = requests.get(url, stream=True)
    with open(f'data/{date}/{file_name}', 'wb') as f:
      for chunk in response.iter_content(chunk_size=8192):
        if chunk:
          f.write(chunk)

    print(f"Downloaded {file_name}")
  except Exception as e:
    print(f"Error downloading {url}: {e}")


def download_all(date, urls):
  threads = []

  for url in urls:
    thread = threading.Thread(target=download_url, args=(date, url,))
    thread.start()
    threads.append(thread)

  for thread in threads:
    thread.join()


def download_tiles_and_merge():
  date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
  for i in date_list:
    current_date = i.strftime("%Y-%m-%d")
    earthaccess.login(strategy="netrc")
    results = earthaccess.search_data(short_name="MOD10A1", cloud_hosted=True,
      bounding_box=(-124.77, 24.52, -66.95, 49.38), temporal=(current_date, current_date)
      # start date and end date are same to consider 1 day
    )
    current_working_dir = os.getcwd()
    earthaccess.download(results, input_folder)
    convert_all_hdf_in_folder(input_folder, output_folder)
    exit()


download_tiles_and_merge()
