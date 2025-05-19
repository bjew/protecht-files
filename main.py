import kagglehub
from kagglehub import KaggleDatasetAdapter
from sqlalchemy import create_engine
import os
import logging

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv('S3_BUCKET_NAME')
KAGGLE_HANDLE = os.getenv('KAGGLE_HANDLE')
KAGGLE_PATH = os.getenv('KAGGLE_PATH')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')

# download dataset from kaggle
try:
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        KAGGLE_HANDLE,
        KAGGLE_PATH,
    )
except Exception as e:
    logger.error(f"Error downloading dataset: {e}")
    raise

# code for writing df to s3 file
# file_name = 'forex.csv'
# df.to_csv(f's3://{S3_BUCKET}/{file_name}', index=False)

# dataframe to postgres database
try:
    engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_USERNAME}')
    df.to_sql('forex', engine, if_exists='replace', index=False)
    logger.info("DataFrame written to PostgreSQL database successfully.")
except Exception as e:
    logger.error(f"Error writing DataFrame to PostgreSQL database: {e}")
    raise