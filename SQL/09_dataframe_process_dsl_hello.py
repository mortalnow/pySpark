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

    df.select(["id", "subject"]).show()
    df.select("id", "subject").show()

    df.filter("score < 99").show()
    df.filter(df['score'] < 99).show()

    df.where("score < 99").show()
    df.where(df['score'] < 99).show()

    df.groupBy("subject").count().show()



