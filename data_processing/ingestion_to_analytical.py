import os
import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType
import boto3

def read_ingestion_data(spark, input_dir, audit_dir):
    """
    Read data from the ingestion layer and write audit information to the audit layer.
    """
    try:
        # Read Parquet files from ingestion layer with increased parallelism
        df = spark.read.parquet(input_dir).repartition(spark.sparkContext.defaultParallelism * 2)
        
        # Write audit information to the audit layer
        write_audit_data(spark, audit_dir, "read_ingestion", df.count(), current_timestamp())
        
        return df
    except Exception as e:
        logging.error(f"Error reading ingestion data: {str(e)}")
        raise

def write_data_to_s3(df, output_path):
    """
    Write DataFrame to S3.
    """
    try:
        # Write DataFrame to Parquet format in S3
        df.write.mode("overwrite").parquet(output_path)
    except Exception as e:
        logging.error(f"Error writing DataFrame to S3: {str(e)}")
        raise

def write_audit_data(spark, audit_dir, operation, count, timestamp):
    """
    Write audit data to the audit layer.
    """
    try:
        # Create audit DataFrame
        audit_df = spark.createDataFrame([(operation, count, timestamp)], ["operation", "count", "timestamp"])
        
        # Write audit DataFrame to Parquet format in S3
        write_data_to_s3(audit_df, audit_dir)
    except Exception as e:
        logging.error(f"Error writing audit data to S3: {str(e)}")
        raise

def send_email_notification(subject, message):
    """
    Send email notification in case of failures.
    """
    # Configure Boto3 SES client
    ses_client = boto3.client("ses", region_name="your-region")
    try:
        # Send email
        response = ses_client.send_email(
            Destination={
                "ToAddresses": ["recipient@example.com"],
            },
            Message={
                "Body": {
                    "Text": {
                        "Charset": "UTF-8",
                        "Data": message,
                    },
                },
                "Subject": {
                    "Charset": "UTF-8",
                    "Data": subject,
                },
            },
            Source="sender@example.com",
        )
        logging.info("Email notification sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email notification: {str(e)}")
        raise

def correlate_data(ad_impressions_df, clicks_df, conversions_df):
    """
    Correlate ad impressions with clicks and conversions.
    """
    try:
        # Join ad impressions with clicks on user ID and timestamp
        ad_impressions_clicks = ad_impressions_df.join(
            clicks_df,
            (ad_impressions_df["user_id"] == clicks_df["user_id"]) & 
            (ad_impressions_df["timestamp"] == clicks_df["timestamp"]),
            "left_outer"
        ).select(
            ad_impressions_df["*"],
            clicks_df["click_id"]
        )
        
        # Join ad impressions with conversions on user ID and timestamp
        ad_impressions_conversions = ad_impressions_df.join(
            conversions_df,
            (ad_impressions_df["user_id"] == conversions_df["user_id"]) & 
            (ad_impressions_df["timestamp"] == conversions_df["timestamp"]),
            "left_outer"
        ).select(
            ad_impressions_df["*"],
            conversions_df["conversion_id"]
        )
        
        return ad_impressions_clicks, ad_impressions_conversions
    except Exception as e:
        logging.error(f"Error correlating data: {str(e)}")
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
    ingestion_dir = "s3://your-bucket-name/ingestion"
    analytical_dir = "s3://your-bucket-name/analytical"
    audit_dir = "s3://your-bucket-name/audit"
    
    try:
        # Read data from ingestion layer
        ad_impressions_df = read_ingestion_data(spark, os.path.join(ingestion_dir, "ad_impressions"), audit_dir)
        clicks_df = read_ingestion_data(spark, os.path.join(ingestion_dir, "clicks"), audit_dir)
        conversions_df = read_ingestion_data(spark, os.path.join(ingestion_dir, "conversions"), audit_dir)
        
        # Correlate data
        ad_impressions_clicks, ad_impressions_conversions = correlate_data(ad_impressions_df, clicks_df, conversions_df)
        
        # Write correlated data to analytical layer
        write_data_to_s3(ad_impressions_clicks, os.path.join(analytical_dir, "ad_impressions_clicks"))
        write_data_to_s3(ad_impressions_conversions, os.path.join(analytical_dir, "ad_impressions_conversions"))
        
        logging.info("Correlated data written to the analytical layer successfully.")
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        send_email_notification("Data Processing Failed", f"Error: {str(e)}")
        # Exit with non-zero status code to indicate job failure
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Run main function
    main()
