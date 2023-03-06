from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F

if __name__ == "__main__":
    # build SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    # get sparkContext by SparkSession
    sc = spark.sparkContext

    # SQL style
    rdd = sc.textFile("../Data/words.txt").\
        flatMap(lambda x: x.split(" ")).\
        map(lambda x: [x])
    # create dataframe by rdd
    df = rdd.toDF(["word"])
    # create table
    df.createTempView("words")
    # query the table
    spark.sql("select word, count(*) as cnt from words group by 1 order by 2 desc").show()


    # DSL style
    df = spark.read.format("text").load("../Data/words.txt")
    # use withColumn reframe data
    df2 = df.withColumn("value", F.explode(F.split(df["value"], " ")))

    df2.groupBy("value").\
        count().\
        withColumnRenamed("value", "word"). \
        withColumnRenamed("count", "cnt").\
        orderBy("cnt", ascending=False).\
        show()









