import os
import zipfile
from dotenv import load_dotenv
load_dotenv()
from kaggle.api.kaggle_api_extended import KaggleApi

def extract_data():
    # Initialize Kaggle API client
    api = KaggleApi()
    api.authenticate()

    try:
        dataset = "waqi786/remote-work-and-mental-health"
        data_path = "data/raw/remote-work-and-mental-health.zip"
        api.dataset_download_files(dataset, path="data/raw", unzip=False)


        with zipfile.ZipFile(data_path, 'r') as zip_ref:
            zip_ref.extractall("data/raw")
    except Exception as e:
        print("Error while extracting data" + e)

    print("Dataset downloaded and extracted successfully.")
