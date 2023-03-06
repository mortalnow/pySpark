from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

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

    df1 = rdd.toDF(["name", "age"])
    df1.printSchema()
    df1.show()

    schema = StructType().add("name", StringType(), True). \
        add("age", IntegerType(), False)
    df2 = rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()

