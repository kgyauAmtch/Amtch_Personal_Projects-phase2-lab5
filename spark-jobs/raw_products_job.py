import sys
import boto3
import pandas as pd
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType
)

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ------------------- Schema -------------------
def get_products_schema():
    return StructType([
        StructField("product_id", IntegerType(), nullable=False),
        StructField("department_id", IntegerType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=False)
    ])

# ------------------- Main Logic -------------------
def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    s3 = boto3.client('s3')
    bucket = 'lakehouse-lab5'
    key = 'raw_landing_zone/products.csv'
    staging_path = 's3://lakehouse-lab5/lakehouse-dwh/staging/products'

    try:
        log.info("Checking for file existence...")
        s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        log.error(f"File not found: {key} - {e}")
        job.commit()
        return

    try:
        log.info(f"Reading CSV file: {key}")
        csv_path = f"s3://{bucket}/{key}"
        df = pd.read_csv(csv_path)

        if df.empty:
            raise Exception("CSV file is empty")

        df['processed_at'] = pd.Timestamp.now()

        schema = get_products_schema()
        spark_df = spark.createDataFrame(df, schema=schema)

        log.info(f"Writing to Delta Lake staging path: {staging_path}")
        spark_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(staging_path)

        log.info("\u2713 Products raw to staging completed successfully")

    except Exception as e:
        log.error(f"Error during products raw to staging: {e}")

    job.commit()
    sc.stop()

if __name__ == "__main__":
    main()
