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
modis_day_wise = os.getcwd() + "/final_output/"
os.makedirs(output_folder, exist_ok=True)
os.makedirs(modis_day_wise, exist_ok=True)


def convert_hdf_to_geotiff(hdf_file, output_folder):
  hdf_ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)

  # Specific subdataset name you're interested in
  target_subdataset_name = "MOD_Grid_Snow_500m:NDSI_Snow_Cover"

  for subdataset in hdf_ds.GetSubDatasets():
    # Check if the subdataset is the one we want to convert
    if target_subdataset_name in subdataset[0]:
      ds = gdal.Open(subdataset[0], gdal.GA_ReadOnly)

      # Create a name for the output file based on the HDF file name and subdataset
      output_file_name = os.path.splitext(os.path.basename(hdf_file))[0] + ".tif"
      output_path = os.path.join(output_folder, output_file_name)

      # Convert to GeoTIFF
      gdal.Translate(output_path, ds)
      ds = None
      break  # Exit the loop after converting the target subdataset

  hdf_ds = None


def convert_all_hdf_in_folder(folder_path, output_folder):
  file_lst = list()
  for file in os.listdir(folder_path):
    file_lst.append(file)
    if file.lower().endswith(".hdf"):
      hdf_file = os.path.join(folder_path, file)
      convert_hdf_to_geotiff(hdf_file, output_folder)
      print(f"Converted {file} to GeoTIFF")
  return file_lst


def merge_tifs(folder_path, output_file):
  tif_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.tif')]
  # gdal_command = ['gdal_merge.py', '-o', output_file, '-of', 'GTiff', '-r', 'cubic'] + tif_files
  gdal_command = gdal_command = ['gdalwarp', '-r', 'cubic'] + tif_files + [output_file]
  subprocess.run(gdal_command)


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


def delete_files_in_folder(folder_path):
  if not os.path.exists(folder_path):
    print("Folder does not exist.")
    return

  for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    try:
      if os.path.isfile(file_path) or os.path.islink(file_path):
        os.unlink(file_path)
      else:
        print(f"Skipping {filename}, as it is not a file.")
    except Exception as e:
      print(f"Failed to delete {file_path}. Reason: {e}")


def download_tiles_and_merge():
  date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
  for i in date_list:
    current_date = i.strftime("%Y-%m-%d")
    earthaccess.login(strategy="netrc")
    results = earthaccess.search_data(short_name="MOD10A1", cloud_hosted=True, bounding_box=(-124.77, 24.52, -66.95, 49.38),
                            temporal=(current_date, current_date))
    earthaccess.download(results, input_folder)
    convert_all_hdf_in_folder(input_folder, output_folder)
    merge_tifs(folder_path=output_folder, output_file=f'{modis_day_wise}/{current_date}__snow_cover.tif')
    delete_files_in_folder(input_folder)  # cleanup
    delete_files_in_folder(output_folder)  # cleanup

download_tiles_and_merge()
