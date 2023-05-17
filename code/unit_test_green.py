import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
from green import (
    read_csv_files,
    handle_null_values_and_drop_duplicates,
    add_extra_columns,
    check_data_quality,
    write_data_to_delta
)
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

class SparkTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = SparkSession.builder \
            .appName("UnitTesting") \
            .master("local[*]") \
            .getOrCreate()
        cls.schema = StructType([
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

        cls.file_with_header_path = os.getenv("FILE_WITH_HEADER_PATH", "<DEFAULT_VALUE>")
        cls.file_without_header_path = os.getenv("FILE_WITHOUT_HEADER_PATH", "<DEFAULT_VALUE>")

    def test_read_csv_files(self):
        df_with_header, df_without_header = read_csv_files(
            self.spark, self.schema, self.file_with_header_path, self.file_without_header_path
        )
        self.assertIsNotNone(df_with_header)
        self.assertIsNotNone(df_without_header)

    def test_handle_null_values_and_drop_duplicates(self):
        df = self.spark.createDataFrame([(1, 2, None), (3, None, 4)], ["A", "B", "C"])
        processed_df = handle_null_values_and_drop_duplicates(df)
        self.assertEqual(processed_df.count(), 2)
        self.assertEqual(len(processed_df.columns), 3)

    def test_add_extra_columns(self):
        df = self.spark.createDataFrame([(1, 2)], ["A", "B"])
        batch_id = "123"
        processed_df = add_extra_columns(df, batch_id)
        self.assertEqual(len(processed_df.columns), 4)
        self.assertTrue("ingestion_tms" in processed_df.columns)
        self.assertTrue("batch_id" in processed_df.columns)

    def test_check_data_quality(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["A", "B"])
        self.assertIsNone(check_data_quality(df))

    def test_write_data_to_delta(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["A", "B"])
        output_path = "path/to/delta_table"
        write_data_to_delta(df, output_path)
        # Perform assertions to check if data is written to Delta table correctly


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        super().tearDownClass()

if __name__ == "__main__":
    unittest.main()
