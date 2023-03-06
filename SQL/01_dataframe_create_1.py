from pyspark.sql import SparkSession

if __name__ == "__main__":
    # build SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # get sparkContext by SparkSession
    sc = spark.sparkContext

    rdd = sc.textFile("../Data/sql/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))

    df = spark.createDataFrame(rdd, schema=['name', 'age'])

    df.printSchema()
    df.show(20)

    df.createOrReplaceTempView("people")
    spark.sql("select * from people where age>=30").show()

