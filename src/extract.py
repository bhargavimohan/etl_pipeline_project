from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile


def extract_data(api_path, zipped_path):
    # Initialize Kaggle API client
    api = KaggleApi()
    api.authenticate()

    try:
        api.dataset_download_files(api_path, path="data/raw", unzip=False)
        with zipfile.ZipFile(zipped_path, "r") as zip_ref:
            zip_ref.extractall("data/raw")
    except Exception as e:
        print("Error while extracting data" + e)

    print("Dataset downloaded and extracted successfully.")
