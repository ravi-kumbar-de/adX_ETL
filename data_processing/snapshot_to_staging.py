import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType

def read_json_data(spark, input_path, schema_file):
    """
    Read JSON data from the specified input path using the given schema.
    """
    try:
        # Read schema from external file
        with open(schema_file, 'r') as file:
            schema = StructType.fromJson(json.load(file))
        
        # Read JSON data with specified schema
        df = spark.read.schema(schema).json(input_path)
        
        # Convert all columns to string type
        for column in df.columns:
            df = df.withColumn(column, col(column).cast(StringType()))
        
        return df
    except Exception as e:
        logging.error(f"Error reading JSON data: {str(e)}")
        raise

def read_csv_data(spark, input_path, header=True):
    """
    Read CSV data from the specified input path.
    """
    try:
        # Read CSV data
        df = spark.read.csv(input_path, header=header)
        
        # Convert all columns to string type
        for column in df.columns:
            df = df.withColumn(column, col(column).cast(StringType()))
        
        return df
    except Exception as e:
        logging.error(f"Error reading CSV data: {str(e)}")
        raise

def read_avro_data(spark, input_path):
    """
    Read Avro data from the specified input path.
    """
    try:
        # Read Avro data
        df = spark.read.format("avro").load(input_path)
        
        # Flatten the Avro DataFrame if necessary
        
        # Convert all columns to string type
        for column in df.columns:
            df = df.withColumn(column, col(column).cast(StringType()))
        
        return df
    except Exception as e:
        logging.error(f"Error reading Avro data: {str(e)}")
        raise

# Main function
def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DataIngestion") \
        .getOrCreate()
    
    # Define input directory
    input_dir = "path/to/input/directory"
    
    # Define schema file path
    schema_file = "path/to/schema/file.json"
    
    # Define output directory
    output_dir = "path/to/output/directory"
    
    try:
        # Read JSON data
        json_df = read_json_data(spark, os.path.join(input_dir, "json_data"), schema_file)
        # Write JSON data to output directory
        json_df.write.mode("overwrite").parquet(os.path.join(output_dir, "json_data_parquet"))
        
        # Read CSV data
        csv_df = read_csv_data(spark, os.path.join(input_dir, "csv_data.csv"))
        # Write CSV data to output directory
        csv_df.write.mode("overwrite").parquet(os.path.join(output_dir, "csv_data_parquet"))
        
        # Read Avro data
        avro_df = read_avro_data(spark, os.path.join(input_dir, "avro_data.avro"))
        # Write Avro data to output directory
        avro_df.write.mode("overwrite").parquet(os.path.join(output_dir, "avro_data_parquet"))
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
