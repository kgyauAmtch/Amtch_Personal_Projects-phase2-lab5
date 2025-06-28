import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ------------------- Config -------------------
def get_config():
    return {
        'bucket': 'lakehouse-lab5',
        'staging_path': 's3://lakehouse-lab5/lakehouse-dwh/staging',
        'processed_path': 's3://lakehouse-lab5/lakehouse-dwh/processed'
    }


# ------------------- Validation -------------------
def validate_primary_keys(df):
    return df.filter(col("product_id").isNotNull() & col("product_name").isNotNull())

# ------------------- Deduplication -------------------
def deduplicate_products(df):
    return df.dropDuplicates(["product_id"])

# ------------------- Merge to Delta -------------------
def merge_upsert(spark, df, path):
    try:
        delta_table = DeltaTable.forPath(spark, path)
        (delta_table.alias("target")
         .merge(df.alias("source"), "target.product_id = source.product_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    except:
        df.write.format("delta").mode("overwrite").save(path)

# ------------------- Main -------------------
def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    config = get_config()

    log.info("Loading staging products data...")
    df = spark.read.format("delta").load(f"{config['staging_path']}/products")

    log.info("Applying primary key validation...")
    df = validate_primary_keys(df)

    log.info("Deduplicating products...")
    df = deduplicate_products(df)

    log.info("Merging into processed Delta table...")
    merge_upsert(spark, df, f"{config['processed_path']}/products")

    log.info("✓ Products staging to processed completed successfully")
    sc.stop()

if __name__ == "__main__":
    main()
