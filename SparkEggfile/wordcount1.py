from pyspark.sql import *
from SparkEggfile.utils1 import *
import sys

spark=getsparksession()
lines=spark.sparkContext.textFile("C:/Users/sweta/OneDrive/Desktop/chat.txt")
words = lines.flatMap(lambda line: line.split(" "))
wordCounts = words.countByValue()
print(wordCounts)
