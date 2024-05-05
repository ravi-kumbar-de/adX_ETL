import os
import logging
from pyspark.sql import SparkSession

def connect_to_source(source_type, input_dir):
    """
    Connect to the specified data source and return the file paths.
    """
    if source_type.lower() == 'ad_impressions':
        return os.path.join(input_dir, 'ad_impressions.json')
    elif source_type.lower() == 'clicks_and_conversions':
        return os.path.join(input_dir, 'clicks_and_conversions.csv')
    elif source_type.lower() == 'bid_requests':
        return os.path.join(input_dir, 'bid_requests.avro')
    else:
        raise ValueError("Invalid source type")

def ingest_ad_impressions(spark, input_path):
    """
    Ingest ad impressions data in JSON format.
    """
    try:
        df = spark.read.json(input_path)
        # Perform any required transformations
        return df
    except Exception as e:
        logging.error(f"Error ingesting ad impressions data: {str(e)}")
        raise

def ingest_clicks_and_conversions(spark, input_path):
    """
    Ingest clicks and conversions data in CSV format.
    """
    try:
        df = spark.read.csv(input_path, header=True)
        # Perform any required transformations
        return df
    except Exception as e:
        logging.error(f"Error ingesting clicks and conversions data: {str(e)}")
        raise

def ingest_bid_requests(spark, input_path):
    """
    Ingest bid requests data in Avro format.
    """
    try:
        df = spark.read.format("avro").load(input_path)
        # Perform any required transformations
        return df
    except Exception as e:
        logging.error(f"Error ingesting bid requests data: {str(e)}")
        raise

def setup_logging():
    """
    Setup logging configuration.
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        filename='data_ingestion.log',
                        filemode='a')

def main():
    # Setup logging
    setup_logging()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DataIngestion") \
        .getOrCreate()

    # Define input directory
    input_dir = "path/to/input/directory"

    # Define data sources and their types
    data_sources = {
        'ad_impressions': 'JSON',
        'clicks_and_conversions': 'CSV',
        'bid_requests': 'Avro'
    }

    # Ingest data from each source
    for source_type, file_format in data_sources.items():
        input_path = connect_to_source(source_type, input_dir)
        if file_format == 'JSON':
            df = ingest_ad_impressions(spark, input_path)
        elif file_format == 'CSV':
            df = ingest_clicks_and_conversions(spark, input_path)
        elif file_format == 'Avro':
            df = ingest_bid_requests(spark, input_path)
        else:
            logging.error(f"Invalid file format for {source_type}")
            continue
        
        # Write data to snapshot layer or perform further processing
        
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
