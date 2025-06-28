import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_timestamp, to_date, row_number
from pyspark.sql.window import Window

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ------------------- Config -------------------
def get_config():
    return {
        'bucket': 'lakehouse-lab5',
        'staging_path': 's3://lakehouse-lab5/lakehouse-dwh/staging',
        'processed_path': 's3://lakehouse-lab5/lakehouse-dwh/processed',
        'timestamp_format': "yyyy-MM-dd'T'HH:mm:ss"
    }
# ------------------- Deduplication -------------------
def deduplicate_order_items(df):
    window = Window.partitionBy("order_id", "product_id", "add_to_cart_order") \
                  .orderBy(col("order_timestamp").desc())
    return df.withColumn("row_num", row_number().over(window)) \
             .filter("row_num = 1").drop("row_num")

# ------------------- Validation -------------------
def validate_primary_keys(df):
    return df.filter(col("order_id").isNotNull() & col("product_id").isNotNull())

def convert_timestamps(df, fmt):
    return df.withColumn("order_timestamp", to_timestamp("order_timestamp", fmt))

# ------------------- Merge to Delta -------------------
def merge_upsert(spark, df, path):
    try:
        delta_table = DeltaTable.forPath(spark, path)
        (delta_table.alias("target")
         .merge(df.alias("source"),
                "target.order_id = source.order_id AND target.product_id = source.product_id AND target.add_to_cart_order = source.add_to_cart_order")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    except:
        df.write.format("delta").mode("overwrite").partitionBy("date").save(path)
