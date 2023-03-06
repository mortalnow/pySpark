from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F
import time

if __name__ == "__main__":
    # build SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    # get sparkContext by SparkSession
    sc = spark.sparkContext

    # load data
    schema = StructType().add("user_id", StringType(), True).\
        add("movie_id", IntegerType(), True).\
        add("rank", IntegerType(), True).\
        add("ts", StringType(), True)
    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False). \
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("../Data/sql/u.data")

    # avg rank by user
    df.groupBy("user_id").avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank", 2)).\
        orderBy("avg_rank", ascending=False).\
        show(5)

    # avg rank by movie
    df.groupBy("movie_id").avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank", 2)).\
        orderBy("avg_rank", ascending=False).\
        show(5)

    df.createTempView("movie")
    spark.sql("select movie_id, round(avg(rank),2) as avg_rank from movie group by movie_id order by avg_rank desc limit 5").show()

    # 大于电影平均分的数量
    print("大于电影平均分的数量:", df.where(df["rank"] > df.select(F.avg(df["rank"])).first()["avg(rank)"]).count())

    time.sleep(10000)














