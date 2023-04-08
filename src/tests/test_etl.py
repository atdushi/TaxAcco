
import unittest
from pyspark.sql import SparkSession

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


    def test_etl(self):
        self.assertTrue(True)        

if __name__ == '__main__':
    unittest.main()