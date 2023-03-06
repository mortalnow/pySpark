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

    df = spark.read.format("csv"). \
        schema("id INT, subject STRING, score INT").\
        load("../Data/sql/stu_score.txt")

    df.createTempView("score")
    df.createOrReplaceTempView("score2")
    df.createGlobalTempView("score3")

    spark.sql("select subject, avg(score) from score group by subject order by 1 desc").show()

