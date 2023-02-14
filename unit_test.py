import unittest
from pyspark.sql import SparkSession

from output.solution import getProcessedData, getMerged, findBeef, findAvgCookingTime

class TestFunctions(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("sparkTest1").getOrCreate()

    def test_getProcessedData(self):
        filepath = r'..\input\recipes-000.json'
        df = getProcessedData(filepath)
        self.assertEqual(df.columns, ['ingredients', 'total_cook_time'])

    def test_getMerged(self):
        df = getMerged()
        self.assertEqual(df.columns, ['ingredients', 'total_cook_time'])

    def test_findBeef(self):
        df = findBeef()
        self.assertEqual(df.columns, ['ingredients', 'total_cook_time'])

    def test_findAvgCookingTime(self):
        df = findAvgCookingTime()
        self.assertEqual(df.columns, ['difficulty', 'avg_total_cooking_time'])