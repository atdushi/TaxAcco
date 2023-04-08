
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DateType, StructField, DoubleType
from datetime import datetime
from main.etl import get_latest_rates


class SparkETLTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("PySpark-unit-test")
                      .getOrCreate())

    @classmethod
    def tearDownClass(self):
        self.spark.stop()

    def test_get_latest_rates(self):
        input_schema = StructType([
            StructField('Currency', StringType(), True),
            StructField('RateDate', DateType(), True),
            StructField('Rate', DoubleType(), True)
        ])

        input_data = [("USD", datetime.strptime("2020-01-01", '%Y-%m-%d'), 80.23),
                      ("USD", datetime.strptime("2020-01-02", '%Y-%m-%d'), 79.),
                      ("USD", datetime.strptime("2020-01-03", '%Y-%m-%d'), 81.23)]

        input_df = self.spark.createDataFrame(
            data=input_data, schema=input_schema)

        expected_data = [("USD", datetime.strptime(
            "2020-01-03", '%Y-%m-%d'), 81.23)]

        expected_df = self.spark.createDataFrame(
            data=expected_data, schema=input_schema)

        transformed_df = get_latest_rates(input_df)

        self.assertEqual(sorted(expected_df.collect()),
                         sorted(transformed_df.collect()))


if __name__ == '__main__':
    unittest.main()
