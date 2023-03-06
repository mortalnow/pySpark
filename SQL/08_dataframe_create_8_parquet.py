from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == "__main__":
    # build SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    # get sparkContext by SparkSession
    sc = spark.sparkContext
 
    df = spark.read.format("parquet").load("../Data/sql/users.parquet")
    df.printSchema()
    df.show()





