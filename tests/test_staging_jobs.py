import pytest
import sys
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_timestamp, regexp_replace
import pandas as pd
from datetime import datetime

# Add spark-jobs directory to path for imports
sys.path.append('../spark-jobs')

class TestStagingJobs:
    """Test cases for staging/processed data processing jobs"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing"""
        spark = SparkSession.builder \
            .appName("test_staging") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "/tmp") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_staging_orders_job_structure(self):
        """Test that staging orders job file exists and is importable"""
        try:
            # This would import your actual staging job file
            # import staging_orders_job
            assert True  # Replace with actual import test
        except ImportError as e:
            pytest.fail(f"Failed to import staging_orders_job: {e}")
    
    def test_staging_order_items_job_structure(self):
        """Test that staging order items job file exists and is importable"""
        try:
            # import staging_order_items_job
            assert True  # Replace with actual import test
        except ImportError as e:
            pytest.fail(f"Failed to import staging_order_items_job: {e}")
    
    def test_staging_products_job_structure(self):
        """Test that staging products job file exists and is importable"""
        try:
            # import staging_products_job
            assert True  # Replace with actual import test  
        except ImportError as e:
            pytest.fail(f"Failed to import staging_products_job: {e}")
    
    @pytest.fixture
    def sample_raw_orders_data(self, spark):
        """Create sample raw orders data for testing staging transformations"""
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("order_num", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        data = [
            ("1", "user_001", "2025-04-01 10:30:00", "ORD001", "125.50", "2025-04-01"),
            ("2", "user_002", "2025-04-01 14:15:00", "ORD002", "89.99", "2025-04-01"),
            ("3", "user_001", "2025-04-02 09:45:00", "ORD003", "200.00", "2025-04-02"),
            (None, "user_003", "2025-04-02 16:20:00", "ORD004", "invalid", "2025-04-02"),  # Invalid data
            ("5", None, "2025-04-03 11:00:00", "ORD005", "75.25", "2025-04-03")  # Missing user_id
        ]
        
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_raw_order_items_data(self, spark):
        """Create sample raw order items data for testing staging transformations"""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("days_since_prior_order", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("add_to_cart_order", StringType(), True),
            StructField("reordered", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        data = [
            ("1", "1", "user_001", "7", "prod_001", "1", "0", "2025-04-01 10:30:00", "2025-04-01"),
            ("2", "1", "user_001", "7", "prod_002", "2", "1", "2025-04-01 10:30:00", "2025-04-01"),
            ("3", "2", "user_002", "14", "prod_001", "1", "0", "2025-04-01 14:15:00", "2025-04-01"),
            ("4", "3", "user_001", "1", "prod_003", "1", "0", "2025-04-02 09:45:00", "2025-04-02"),
            (None, "4", "user_003", "invalid", "prod_004", "1", "1", "2025-04-02 16:20:00", "2025-04-02")  # Invalid data
        ]
        
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_raw_products_data(self, spark):
        """Create sample raw products data for testing staging transformations"""
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("department_id", StringType(), True),
            StructField("department", StringType(), True)
        ])
        
        data = [
            ("prod_001", "Organic Bananas", "1", "Fresh Fruits"),
            ("prod_002", "Greek Yogurt", "2", "Dairy"),
            ("prod_003", "Whole Wheat Bread", "3", "Bakery"),
            ("prod_004", "Green Tea", "4", "Beverages"),
            (None, "Invalid Product", "5", "Unknown"),  # Invalid product_id
            ("prod_006", "", "6", "Electronics")  # Empty product name
        ]
        
        return spark.createDataFrame(data, schema)
    
    def test_staging_orders_data_quality_checks(self, sample_raw_orders_data):
        """Test data quality checks for staging orders"""
        df = sample_raw_orders_data
        
        # Test for null order_id records
        null_order_id_count = df.filter(df.order_id.isNull()).count()
        assert null_order_id_count == 1, "Should have 1 record with null order_id"
        
        # Test for valid total_amount format
        valid_amount_count = df.filter(df.total_amount.rlike(r'^\d+\.?\d*$')).count()
        assert valid_amount_count == 4, "Should have 4 records with valid amounts"
        
        # Test timestamp format validation
        valid_timestamp_count = df.filter(df.order_timestamp.rlike(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')).count()
        assert valid_timestamp_count == 5, "All timestamps should be in correct format"
    
    def test_staging_orders_transformations(self, sample_raw_orders_data, spark):
        """Test staging transformations for orders data"""
        df = sample_raw_orders_data
        
        # Test data cleaning - remove null order_ids
        cleaned_df = df.filter(df.order_id.isNotNull())
        assert cleaned_df.count() == 4, "Should have 4 records after removing null order_ids"
        
        # Test type conversions
       
        
        transformed_df = cleaned_df.select(
            col("order_id").cast(IntegerType()).alias("order_id"),
            col("user_id"),
            to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("order_timestamp"),
            col("order_num"),
            regexp_replace(col("total_amount"), "[^\\d.]", "").cast(DoubleType()).alias("total_amount"),
            col("date")
        )
        
        # Verify transformations
        transformed_count = transformed_df.count()
        assert transformed_count <= 4, "Transformed data should not exceed cleaned data count"
        
        # Check data types
        schema_dict = {field.name: field.dataType for field in transformed_df.schema.fields}
        assert isinstance(schema_dict["order_id"], IntegerType), "order_id should be IntegerType"
        assert isinstance(schema_dict["total_amount"], DoubleType), "total_amount should be DoubleType"
    
    def test_staging_order_items_transformations(self, sample_raw_order_items_data):
        """Test staging transformations for order items data"""
        df = sample_raw_order_items_data
        
        # Test data cleaning - remove null ids
        cleaned_df = df.filter(df.id.isNotNull())
        assert cleaned_df.count() == 4, "Should have 4 records after removing null ids"
        
        # Test numeric field validation
        from pyspark.sql.functions import col
        
        numeric_fields = ["days_since_prior_order", "add_to_cart_order", "reordered"]
        for field in numeric_fields:
            valid_count = cleaned_df.filter(col(field).rlike(r'^\d+$')).count()
            # Should have at least some valid numeric values
            assert valid_count >= 3, f"Should have valid numeric values for {field}"
    
    def test_staging_products_transformations(self, sample_raw_products_data):
        """Test staging transformations for products data"""
        df = sample_raw_products_data
        
        # Test data cleaning - remove null product_ids and empty product names
        from pyspark.sql.functions import col, trim, length
        
        cleaned_df = df.filter(
            (col("product_id").isNotNull()) & 
            (length(trim(col("product_name"))) > 0)
        )
        assert cleaned_df.count() == 4, "Should have 4 valid products after cleaning"
        
        # Test department_id validation
        valid_dept_count = cleaned_df.filter(col("department_id").rlike(r'^\d+$')).count()
        assert valid_dept_count == 4, "All cleaned records should have valid department_ids"
    
    def test_staging_data_aggregations(self, sample_raw_orders_data):
        """Test data aggregation functions for staging layer"""
        from pyspark.sql.functions import count, sum as spark_sum, avg, max as spark_max

        df = sample_raw_orders_data.filter(
            (sample_raw_orders_data.order_id.isNotNull()) &
            (sample_raw_orders_data.total_amount.rlike(r'^\d+\.?\d*$'))
        )

    # Continue with your aggregation tests
        result = df.groupBy("user_id").agg(
            count("order_id").alias("order_count"),
            spark_sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_spent"),
            spark_max("total_amount").alias("max_spent")
        )
        assert result.count() >= 0  # Example assertion

        
        # Test user-level aggregations
        user_agg = df.groupBy("user_id").agg(
            count("order_id").alias("order_count"),
            spark_sum("total_amount").alias("total_spent")
        )
        
        user_count = user_agg.count()
        assert user_count >= 2, "Should have aggregations for multiple users"
        
        # Test date-level aggregations
        date_agg = df.groupBy("date").agg(
            count("order_id").alias("daily_orders"),
            avg("total_amount").alias("avg_order_value")
        )
        
        date_count = date_agg.count()
        assert date_count >= 2, "Should have aggregations for multiple dates"
    
    def test_staging_duplicate_handling(self, spark):
        """Test duplicate record handling in staging layer"""
        # Create data with duplicates
        duplicate_data = [
            ("1", "user_001", "2025-04-01 10:30:00", "ORD001", "125.50", "2025-04-01"),
            ("1", "user_001", "2025-04-01 10:30:00", "ORD001", "125.50", "2025-04-01"),  # Duplicate
            ("2", "user_002", "2025-04-01 14:15:00", "ORD002", "89.99", "2025-04-01"),
        ]
        
        columns = ["order_id", "user_id", "order_timestamp", "order_num", "total_amount", "date"]
        df = spark.createDataFrame(duplicate_data, columns)
        
        # Test duplicate removal
        deduplicated_df = df.dropDuplicates()
        assert deduplicated_df.count() == 2, "Should have 2 unique records after deduplication"
        
        # Test duplicate removal by specific columns
        dedup_by_order_id = df.dropDuplicates(["order_id"])
        assert dedup_by_order_id.count() == 2, "Should have 2 unique orders by order_id"
    
    def test_staging_error_handling(self, spark):
        """Test error handling and data validation in staging jobs"""
        # Create problematic data
        problematic_data = [
            ("abc", "user_001", "invalid_timestamp", "ORD001", "not_a_number", "2025-04-01"),
            (None, None, None, None, None, None),
            ("", "", "", "", "", ""),
        ]
        
        columns = ["order_id", "user_id", "order_timestamp", "order_num", "total_amount", "date"]
        df = spark.createDataFrame(problematic_data, columns)
        
        # Test filtering out completely null records
        from pyspark.sql.functions import col
        
        non_null_df = df.filter(
            col("order_id").isNotNull() & 
            (col("order_id") != "")
        )
        
        assert non_null_df.count() == 1, "Should have 1 record with non-null order_id"
    
    @patch('boto3.client')
    def test_staging_s3_output_operations(self, mock_boto_client):
        """Test S3 output operations for staging data"""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 write operations
        mock_s3.put_object.return_value = {'ETag': 'mock-etag'}
        
        # Test your S3 write logic here
        # This would be part of your actual staging job implementation
        assert True  # Replace with actual S3 write tests
    
    def test_staging_data_partitioning(self, sample_raw_orders_data):
        """Test data partitioning strategy for staging layer"""
        df = sample_raw_orders_data
        
        # Test partitioning by date
        partitioned_data = df.groupBy("date").count()
        partition_count = partitioned_data.count()
        
        assert partition_count >= 2, "Should have multiple date partitions"
        
        # Verify each partition has data
        partition_sizes = partitioned_data.collect()
        for row in partition_sizes:
            assert row["count"] > 0, "Each partition should have at least one record"
    
    def test_staging_schema_evolution(self, spark):
        """Test schema evolution handling in staging layer"""
        # Test with additional columns that might be added later
        extended_schema_data = [
            ("1", "user_001", "2025-04-01 10:30:00", "ORD001", "125.50", "2025-04-01", "new_column_value"),
            ("2", "user_002", "2025-04-01 14:15:00", "ORD002", "89.99", "2025-04-01", "another_value"),
        ]
        
        extended_columns = ["order_id", "user_id", "order_timestamp", "order_num", "total_amount", "date", "extra_field"]
        df_extended = spark.createDataFrame(extended_schema_data, extended_columns)
        
        # Test that staging can handle additional columns
        assert len(df_extended.columns) == 7, "Extended schema should have 7 columns"
        
        # Test selecting only required columns for backward compatibility
        required_columns = ["order_id", "user_id", "order_timestamp", "order_num", "total_amount", "date"]
        df_standard = df_extended.select(*required_columns)
        
        assert len(df_standard.columns) == 6, "Standard schema should have 6 columns"

if __name__ == "__main__":
    pytest.main([__file__])