import logging
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data
import os
import json
from dotenv import load_dotenv

load_dotenv()


# Configure logging
logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

api_data_path = os.getenv("API_DATA_PATH")
zipped_data_path = os.getenv("ZIPPED_DATA_PATH")
local_raw_data_path = os.getenv("LOCAL_RAW_DATA_PATH")
processed_data_path = os.getenv("PROCESSED_DATA_PATH")

columns_to_drop = os.getenv("COLUMNS_TO_DROP", "")
filter_values_str = os.getenv("FILTER_VALUES", "{}")
rows_to_drop = json.loads(filter_values_str)


def main():
    logging.info("ETL Pipeline Started")

    try:
        logging.info("Data Extraction started...")
        extract_data(api_path=api_data_path, zipped_path=zipped_data_path)
        logging.info("Data extraction completed successfully")

        logging.info("Data Transformation in progress...")
        transformed_data = transform_data(
            raw_data_path=local_raw_data_path,
            processed_data_path=processed_data_path,
            columns_to_drop=columns_to_drop,
            rows_to_drop=rows_to_drop,
        )
        logging.info("Data transformation completed successfully")

        logging.info("Loading data...")
        load_data(transformed_data)
        logging.info("Data loading completed successfully")

        logging.info("Basic ETL Pipeline Completed Successfully")

    except Exception as e:
        logging.error(f"ETL Pipeline Failed: {str(e)}")


if __name__ == "__main__":
    main()
