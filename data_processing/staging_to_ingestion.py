import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType

def read_staging_data(spark, input_dir, audit_dir):
    """
    Read data from the staging layer and write audit information to the audit layer.
    """
    try:
        # Read Parquet files from staging layer
        df = spark.read.parquet(input_dir)
        
        # Write audit information to the audit layer
        write_audit_data(spark, audit_dir, "read_staging", df.count(), current_timestamp())
        
        return df
    except Exception as e:
        logging.error(f"Error reading staging data: {str(e)}")
        raise

def convert_data_types(df):
    """
    Convert data types to appropriate types.
    """
    try:
        # Example: Convert timestamp strings to timestamp type
        df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
        return df
    except Exception as e:
        logging.error(f"Error converting data types: {str(e)}")
        raise

def standardize_data(df):
    """
    Standardize the data.
    """
    try:
        # Example: Standardize column values
        # Assume 'website' column needs to be standardized
        df = df.withColumn("website", lit("example.com"))  # Standardize 'website' values
        return df
    except Exception as e:
        logging.error(f"Error standardizing data: {str(e)}")
        raise

def transform_data(df):
    """
    Transform the data.
    """
    try:
        # Example: Apply transformations
        # Assume a transformation is needed on 'user_id' column
        df = df.withColumn("user_id", col("user_id").upper())  # Convert 'user_id' to uppercase
        return df
    except Exception as e:
        logging.error(f"Error transforming data: {str(e)}")
        raise

def filter_data(df, audit_dir):
    """
    Filter the data and write audit information to the audit layer.
    """
    try:
        # Filter data based on conditions
        df_filtered = df.filter(col("user_id").isNotNull())
        
        # Write audit information to the audit layer
        write_audit_data(spark, audit_dir, "filter_data", df_filtered.count(), current_timestamp())
        
        return df_filtered
    except Exception as e:
        logging.error(f"Error filtering data: {str(e)}")
        raise

def deduplicate_data(df, audit_dir):
    """
    Deduplicate the data and write audit information to the audit layer.
    """
    try:
        # Deduplicate data based on columns
        df_deduplicated = df.dropDuplicates(["user_id", "timestamp"])
        
        # Write audit information to the audit layer
        write_audit_data(spark, audit_dir, "deduplicate_data", df_deduplicated.count(), current_timestamp())
        
        return df_deduplicated
    except Exception as e:
        logging.error(f"Error deduplicating data: {str(e)}")
        raise

def perform_full_load(spark, input_dir, output_dir, audit_dir):
    """
    Perform full load from staging to ingestion layer.
    """
    try:
        # Read data from staging layer
        df_staging = read_staging_data(spark, input_dir, audit_dir)
        
        # Apply transformations
        df_transformed = convert_data_types(df_staging)
        df_transformed = standardize_data(df_transformed)
        df_transformed = transform_data(df_transformed)
        
        # Filter the data
        df_filtered = filter_data(df_transformed, audit_dir)
        
        # Deduplicate the data
        df_deduplicated = deduplicate_data(df_filtered, audit_dir)
        
        # Add audit columns for SCD operation
        df_final = df_deduplicated.withColumn("load_date", lit(datetime.now()))
        df_final = df_final.withColumn("is_current", lit(True))
        
        # Write data to ingestion layer
        df_final.write.mode("overwrite").parquet(output_dir)
        
        logging.info("Full load completed successfully.")
    except Exception as e:
        logging.error(f"Error performing full load: {str(e)}")
        raise

def perform_incremental_load(spark, input_dir, output_dir, audit_dir, last_load_date):
    """
    Perform incremental load from staging to ingestion layer.
    """
    try:
        # Read data from staging layer
        df_staging = read_staging_data(spark, input_dir, audit_dir)
        
        # Apply transformations
        df_transformed = convert_data_types(df_staging)
        df_transformed = standardize_data(df_transformed)
        df_transformed = transform_data(df_transformed)
        
        # Filter the data
        df_filtered = filter_data(df_transformed, audit_dir)
        
        # Deduplicate the data
        df_deduplicated = deduplicate_data(df_filtered, audit_dir)
        
        # Filter data based on last load date
        df_incremental = df_deduplicated.filter(col("load_date") > last_load_date)
        
        if not df_incremental.isEmpty():
            # Add audit columns for SCD operation
            df_final = df_incremental.withColumn("load_date", lit(datetime.now()))
            df_final = df_final.withColumn("is_current", lit(True))
            
            # Write data to ingestion layer
            df_final.write.mode("append").parquet(output_dir)
            
            logging.info("Incremental load completed successfully.")
        else:
            logging.info("No new data to load.")
    except Exception as e:
        logging.error(f"Error performing incremental load: {str(e)}")
        raise

def write_audit_data(spark, audit_dir, operation, record_count, operation_timestamp):
    """
    Write audit information to the audit layer.
    """
    try:
        # Create DataFrame for audit information
        audit_data = [(operation, record_count, operation_timestamp)]
        audit_schema = StructType([
            StructField("operation", StringType(), True),
            StructField("record_count", StringType(), True),
            StructField("operation_timestamp", TimestampType(), True)
        ])
        df_audit = spark.createDataFrame(audit_data, schema=audit_schema)
        
        # Write audit data to the audit layer
        df_audit.write.mode("append").parquet(audit_dir)
        
        logging.info("Audit information written to the audit layer.")
    except Exception as e:
        logging.error(f"Error writing audit information: {str(e)}")
        raise

# Main function
def main():

    # Initialize Spark session with optimized configurations
    spark = SparkSession.builder \
        .appName("DataIngestion") \
        .config("spark.sql.shuffle.partitions", 4 * spark.sparkContext.defaultParallelism) \
        .config("spark.default.parallelism", spark.sparkContext.defaultParallelism * 2) \
        .getOrCreate()
    
    
    # Define input and output directories (S3 paths)
    staging_dir = "s3://your-bucket-name/staging"
    ingestion_dir = "s3://your-bucket-name/ingestion"
    audit_dir = "s3://your-bucket-name/audit"
    
    try:
        # Perform full load
        perform_full_load(spark, staging_dir, ingestion_dir, audit_dir)
        
        # Example: Perform incremental load (assuming last load date is available)
        last_load_date = "2024-05-05"
        perform_incremental_load(spark, staging_dir, ingestion_dir, audit_dir, last_load_date)
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
