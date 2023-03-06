from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3], 3)
    print(rdd.fold(10, lambda a, b: a + b))
    rdd = sc.parallelize([1, 2, 3], 2)
    print(rdd.fold(10, lambda a, b: a + b))
