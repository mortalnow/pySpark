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

    schema = StructType().add("data", StringType(), True)
    df = spark.read.format("text").\
        schema(schema=schema).\
        load("../Data/sql/people.txt")

    df.printSchema()
    df.show()




