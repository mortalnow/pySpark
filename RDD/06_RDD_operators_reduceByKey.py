from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("new").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # create a rdd
    rdd = sc.parallelize([('big', 1), ('small', 2), ('small', 1), ('big', 2), ('small', 3), ('big', 5)])

    # reduceByKey add all the values of same key
    print(rdd.reduceByKey(lambda a, b: a + b).collect())
