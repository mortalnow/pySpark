from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 6, 8, 3, 5, 3, 5, 9, 76], 1)

    # default ascending
    print(rdd.takeOrdered(3))

    # descending
    print(rdd.takeOrdered(3, lambda x: -x))
    print(rdd.top(3))
