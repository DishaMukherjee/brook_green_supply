from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
import uuid
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths
file_with_header_path = os.environ['FILE_WITH_HEADER_PATH']
file_without_header_path = os.environ['FILE_WITHOUT_HEADER_PATH']
gen_file_with_header = os.environ['GEN_FILE_WITH_HEADER']
gen_file_without_header = os.environ['GEN_FILE_WITHOUT_HEADER']

# Generate a UUID
batch_id = str(uuid.uuid4())
logging.info(f"Generated batch_id: {batch_id}")

try:
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Ingest CSV to DeltaLake") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    logging.info("Initialized Spark session")

    # Set log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    # Schema of your CSV
    schema = StructType([
        StructField("LatD", DoubleType(), True),
        StructField("LatM", DoubleType(), True),
        StructField("LatS", DoubleType(), True),
        StructField("NS", StringType(), True),
        StructField("LonD", DoubleType(), True),
        StructField("LonM", DoubleType(), True),
        StructField("LonS", DoubleType(), True),
        StructField("EW", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True)
    ])
    logging.info("Defined schema")

    # Read CSV files
    df_with_header = spark.read.schema(schema).option("header", "true").csv(file_with_header_path)
    df_without_header = spark.read.schema(schema).csv(file_without_header_path)
    logging.info("Read CSV files")

    # Handle null values and drop duplicates
    df_with_header = df_with_header.fillna('').dropDuplicates()
    df_without_header = df_without_header.fillna('').dropDuplicates()
    logging.info("Handled null values and dropped duplicates")

    # Add ingestion_tms and batch_id columns, and rename other columns
    def add_extra_columns(df_with_header, df_without_header, batch_id):
        df_with_header = df_with_header.withColumn("ingestion_tms", current_timestamp()) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumnRenamed("LatD", "latitude_degrees") \
            .withColumnRenamed("LatM", "latitude_minutes") \
            .withColumnRenamed("LatS", "latitude_seconds") \
            .withColumnRenamed("NS", "latitude_direction") \
            .withColumnRenamed("LonD", "longitude_degrees") \
            .withColumnRenamed("LonM", "longitude_minutes") \
            .withColumnRenamed("LonS", "longitude_seconds") \
            .withColumnRenamed("EW", "longitude_direction")

        # Create an empty DataFrame for df_without_header
        df_without_header = df_without_header.limit(0)
        df_without_header = df_without_header.withColumn("ingestion_tms", current_timestamp()) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumnRenamed("_c2", "latitude_seconds") \
            .withColumnRenamed("_c3", "latitude_direction") \
            .withColumnRenamed("_c4", "longitude_degrees") \
            .withColumnRenamed("_c5", "longitude_minutes") \
            .withColumnRenamed("_c6", "longitude_seconds") \
            .withColumnRenamed("_c7", "longitude_direction") \
            .withColumnRenamed("_c8", "City") \
            .withColumnRenamed("_c9", "State")

        return df_with_header, df_without_header

    # Call the function to add extra columns
    df_with_header, df_without_header = add_extra_columns(df_with_header, df_without_header, batch_id)
    logging.info("Added ingestion_tms and batch_id columns, and renamed other columns")

    # Interrupt point: Check data quality
    def check_data_quality(df):
        if df.count() > 0:
            return None
        else:
            return "Dataframe is empty"

    # Check data quality
    quality_check_result = check_data_quality(df_with_header)
    logging.info(f"Data Quality check result: {quality_check_result}")

    # Write data to Delta tables
    def write_data_to_delta(df, output_path):
        df.write.format("delta").mode("append") \
            .partitionBy("ingestion_tms", "batch_id") \
            .save(output_path)

    # Write data to Delta tables
    write_data_to_delta(df_with_header, output_path=gen_file_with_header)
    write_data_to_delta(df_without_header, output_path=gen_file_without_header)
    logging.info("CSV files ingested successfully into Delta tables")

except AssertionError as ae:
    logging.error(f"Data Quality Check Failed: {ae}")

except Exception as e:
    logging.error("Error ingesting CSV files", exc_info=True)

finally:
    # Stop the Spark session
    if 'spark' in locals() or 'spark' in globals():
        spark.stop()
    logging.info("Spark session stopped successfully")
