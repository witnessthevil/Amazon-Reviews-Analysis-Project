from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([1.0, 2.0, 3.0], FloatType())
print(type(df))