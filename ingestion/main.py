
from utils.ingestion import APIDataIngestion, CSVDataIngestion
from utils.logger import logger
from utils.storage import DataStorage

# Source paths
csv_path = "../Dataset/Telco-Customer-Churn.csv"
api_url = "https://my.api.mockaroo.com/users"
# Output path
raw_path = "../Dataset/Raw Data"
storage_path = "../Dataset"

API_HEADERS = {"X-API-Key": "2a258740"}

storage = DataStorage(name="Customer Churn Data", storage_root=storage_path)

def ingest_api(api_url, headers, output_dir ):
    logger.info("Starting ingestion")
    _, file = APIDataIngestion.ingest(api_url, output_dir=output_dir, headers=headers)
    logger.info("ingestion Complete")
    logger.info("Starting Data Segregation")
    storage.store(file)
    logger.info("Data Segregation complete")

def ingest_csv(csv_path, output_dir ):
    logger.info("Starting ingestion")
    _, file = CSVDataIngestion.ingest(csv_path, output_dir)
    logger.info("ingestion Complete")
    logger.info("Starting Data Segregation")
    storage.store(file)
    logger.info("Data Segregation complete")

if __name__ == "__main__":
    ingest_api(api_url, API_HEADERS, raw_path)
    ingest_csv(csv_path, raw_path)

