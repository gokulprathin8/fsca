import os
import subprocess
import threading
from datetime import datetime
from datetime import timedelta

import requests
from bs4 import BeautifulSoup

start_date = datetime(2019, 1, 1)
end_date = datetime(2019, 1, 3)
tile_list = ["h08v04", "h08v05", "h09v04", "h09v05", "h10v04", "h10v05", "h11v04", "h11v05", "h12v04", "h12v05",
             "h13v04", "h13v05", "h15v04", "h16v03", "h16v04", ]


def list_files(directory):
  return [os.path.abspath(os.path.join(directory, f)) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]


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
    current_date = i.strftime("%Y.%m.%d")
    url = f"https://n5eil01u.ecs.nsidc.org/MOST/MOD10A1.061/{current_date}/"
    response = requests.get(url)
    if response.status_code == 200:
      soup = BeautifulSoup(response.text, "html.parser")
      links = soup.find_all("a")
      all_hdf_files = [url + link.get("href") for link in links if link.get("href").endswith("hdf")]
      filtered_hdf_files = [hdf_file for hdf_file in all_hdf_files if any(tile in hdf_file for tile in tile_list)]
      download_all(current_date, filtered_hdf_files)

      # perform merge
      merge_tiles(current_date, filtered_hdf_files)


download_tiles_and_merge()
