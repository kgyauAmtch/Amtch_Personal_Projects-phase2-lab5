import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, TimestampType, DateType
)
from pyspark.sql.functions import current_timestamp

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ------------------- Schema -------------------
def get_orders_schema():
    return StructType([
        StructField("order_num", IntegerType(), nullable=True),
        StructField("order_id", IntegerType(), nullable=False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("order_timestamp", TimestampType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("date", DateType(), nullable=False)
    ])

# ------------------- Main Logic -------------------
def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    # Init Spark and Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    input_path = 's3://lakehouse-lab5/preprocess-csv/orders/orders_apr_2025.csv'
    staging_path = 's3://lakehouse-lab5/lakehouse-dwh/staging/orders'

    try:
        log.info(f"Reading CSV from: {input_path}")

        df = spark.read.format("csv") \
            .option("header", "true") \
            .schema(get_orders_schema()) \
            .load(input_path)

        if df.rdd.isEmpty():
            raise Exception("Input CSV file is empty")

        # Add processing timestamp
        df = df.withColumn("processed_at", current_timestamp())

        log.info(f"Writing to Delta Lake staging path: {staging_path}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(staging_path)

        log.info("✓ Orders CSV written to Delta Lake staging successfully")

    except Exception as e:
        log.error(f"Error during orders CSV to staging: {e}")
        raise

    job.commit()
    sc.stop()

if __name__ == "__main__":
    main()
