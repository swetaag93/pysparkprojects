from pyspark.sql import *

def getsparksession():
      spark1 = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[1]") \
        .getOrCreate()
      return spark1
