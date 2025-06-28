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
