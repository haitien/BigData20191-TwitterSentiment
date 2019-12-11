from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from zipfile import ZipFile
import urllib.request


# URL to download the sentiment140 dataset and data file names
DATA_URL = "http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"
TRAIN_FILENAME = "training.1600000.processed.noemoticon.csv"
TEST_FILENAME = "testdata.manual.2009.06.14.csv"
# Folder for storing the downloaded data
DATA_FOLDER = "data"
# Data column names
COL_NAMES = ["label", "id", "date", "query_string", "user", "text"]
# Text encoding type of the data
ENCODING = "iso-8859-1"

def download_data(url, data_folder=DATA_FOLDER, filename="downloaded_data.zip"):
  """Download and extract data from url"""
  
  data_dir = "./" + DATA_FOLDER
  if not os.path.exists(data_dir):
    os.makedirs(data_dir)
  downloaded_filepath = os.path.join(data_dir, filename)
  print("Downloading data...")
  urllib.request.urlretrieve(url, downloaded_filepath)
  print("Extracting data...")
  zipfile = ZipFile(downloaded_filepath)
  zipfile.extractall(data_dir)
  zipfile.close()
  print("Finished data downloading and extraction.")
if __name__ == '__main__':
    download_data(DATA_URL)