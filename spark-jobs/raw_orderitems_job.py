import sys
import boto3
import pandas as pd
import logging
from io import BytesIO
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import (
    StructType, StructField, IntegerType, TimestampType, DateType
)

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ------------------- Schema -------------------
def get_order_items_schema():
    return StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("order_id", IntegerType(), nullable=False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("days_since_prior_order", IntegerType(), nullable=True),
        StructField("product_id", IntegerType(), nullable=False),
        StructField("add_to_cart_order", IntegerType(), nullable=True),
        StructField("reordered", IntegerType(), nullable=True),
        StructField("order_timestamp", TimestampType(), nullable=False),
        StructField("date", DateType(), nullable=False)
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
    key = 'raw_landing_zone/order_items_apr_2025.xlsx'
    staging_path = 's3://lakehouse-lab5/lakehouse-dwh/staging/order_items'

    try:
        log.info("Checking for file existence...")
        s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        log.error(f"File not found: {key} - {e}")
        job.commit()
        return

    try:
        log.info(f"Downloading and reading Excel file: {key}")
        file_obj = s3.get_object(Bucket=bucket, Key=key)
        xls = pd.ExcelFile(BytesIO(file_obj['Body'].read()))

        if not xls.sheet_names:
            raise Exception("No sheets found in Excel file")

        dfs = []
        for sheet in xls.sheet_names:
            df = xls.parse(sheet)
            if not df.empty:
                df['source_sheet'] = sheet
                df['processed_at'] = pd.Timestamp.now()
                dfs.append(df)

        if not dfs:
            raise Exception("No valid data found in any sheet")

        combined_df = pd.concat(dfs, ignore_index=True)

        for col in ['order_timestamp', 'date']:
            if col in combined_df.columns:
                combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')

        schema = get_order_items_schema()
        spark_df = spark.createDataFrame(combined_df, schema=schema)

        log.info(f"Writing to Delta Lake staging path: {staging_path}")
        spark_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(staging_path)

        log.info("✓ Order items raw to staging completed successfully")

    except Exception as e:
        log.error(f"Error during order items raw to staging: {e}")

    job.commit()
    sc.stop()

if __name__ == "__main__":
    main()
