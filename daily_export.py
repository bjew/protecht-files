# daily_export.py
from sqlalchemy import create_engine
import os
import logging
import boto3
import rank_query

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv('S3_BUCKET_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')

engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_USERNAME}')
with engine.connect() as conn:
    # begin a transaction
    with conn.connection.cursor() as curs:
        try:
            query = rank_query.rank_query
            sql = f"COPY ({query}) TO STDOUT WITH NULL AS '#N/A' CSV HEADER"
            with open("daily.csv", "w") as file:
                # execute the query
                curs.copy_expert(sql, file)
        except Exception as e:
            conn.close()
            raise e
    print("export complete")

conn.close()

# Upload the file to S3
s3 = boto3.client('s3')
try:
    s3.upload_file("daily.csv", S3_BUCKET, "daily.csv")
    logger.info("File uploaded to S3 successfully.")
except Exception as e:
    logger.error(f"Error uploading file to S3: {e}")
    raise