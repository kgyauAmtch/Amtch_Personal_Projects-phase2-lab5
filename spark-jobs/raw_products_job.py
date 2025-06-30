import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

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

# ------------------- Main -------------------
def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    # Initialize Glue and Spark
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Hardcoded S3 Input and Output paths
    input_path = 's3://lakehouse-lab5/preprocess-csv/products*.csv'
    staging_path = 's3://lakehouse-lab5/lakehouse-dwh/staging/products'

    try:
        log.info(f"Reading CSV from: {input_path}")
        
        # Read CSV with predefined schema
        df = spark.read.format("csv") \
            .option("header", "true") \
            .schema(get_products_schema()) \
            .load(input_path)

        if df.rdd.isEmpty():
            raise Exception("Input CSV is empty")

        # Add a processing timestamp
        df = df.withColumn("processed_at", current_timestamp())

        log.info(f"Writing to Delta Lake staging path: {staging_path}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(staging_path)

        log.info("✓ Products CSV written to Delta Lake staging successfully")

    except Exception as e:
        log.error(f"Error processing products CSV to staging: {e}")
        raise

    job.commit()
    sc.stop()

if __name__ == "__main__":
    main()


