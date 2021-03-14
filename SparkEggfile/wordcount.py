from pyspark.sql import *
from SparkEggfile.utils1 import *
import sys

if __name__ == "__main__":
        if len(sys.argv)<1:
                print("not executed")
        else:
                spark=getsparksession()
                lines=spark.sparkContext.textFile(sys.argv[0])
                words = lines.flatMap(lambda line: line.split(" "))
                wordCounts = words.countByValue()
                print(wordCounts.collect())
