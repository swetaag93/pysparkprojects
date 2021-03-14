from pyspark.sql import *
from SparkEggfile.utils1 import *
import sys

spark=getsparksession()
emp=spark.sparkContext.textFile("C:/Users/sweta/OneDrive/Desktop/employee.txt")
print(emp.collect())
dept=spark.sparkContext.textFile("C:/Users/sweta/OneDrive/Desktop/department.txt")
print(dept.collect())
emp1=emp.map(lambda x: (x.split(",")[1],x.split(",")[0]))
print(emp1.collect())
dept1=dept.map(lambda x: (x.split(",")[0],x.split(",")[1]))
ed=emp1.join(dept1)
print(ed.collect())
