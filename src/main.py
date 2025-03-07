import logging
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

# Configure logging
logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    logging.info("ETL Pipeline Started")

    try:
        logging.info("Data Extraction started...")
        raw_data = extract_data()
        logging.info("Data extraction completed successfully")

        logging.info("Data Transformation in progress...")
        transformed_data = transform_data(raw_data)
        logging.info("Data transformation completed successfully")

        logging.info("Loading data...")
        load_data(transformed_data)
        logging.info("Data loading completed successfully")

        logging.info("Basic ETL Pipeline Completed Successfully")

    except Exception as e:
        logging.error(f"ETL Pipeline Failed: {str(e)}")

if __name__ == "__main__":
    main()
