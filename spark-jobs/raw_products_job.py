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
