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
    StructType, StructField, IntegerType, DoubleType, TimestampType, DateType
)

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
    