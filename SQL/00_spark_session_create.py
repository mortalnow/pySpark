from pyspark.sql import SparkSession

if __name__ == "__main__":
    # build SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # get sparkContext by SparkSession
    sc = spark.sparkContext

    df = spark.read.csv("../Data/stu_score.txt", sep=',', header=False)
    df2 = df.toDF("id", "name", "score")
    df2.printSchema()
    df2.show()

    df2.createTempView("score")

    # sql style
    spark.sql("""
    select * from score where name = "语文" limit 5
    """).show()

    # dsl style
    df2.where("name ='语文'").limit(5).show()
