from pyspark.sql import *
from SparkEggfile.utils1 import *
import sys

def run():
  print("length is",len(sys.argv))
  if len(sys.argv)<2:
    print("not executed")
  else:
    spark=getsparksession()
    lines=spark.sparkContext.textFile(sys.argv[1])
    words = lines.flatMap(lambda line: line.split(" "))
    #words1=words.map(lambda x:(x,1))
    #wordCounts = words1.reduceByKey(lambda x,y:x+y)
    #for word,count in wordCounts.collect():
    #    print("{} : {}".format(word, count))
    #spark.stop()
    word2=words.countByValue()
    for word,count in word2.items():
      print("{} : {}".format(word, count))
      spark.stop()



