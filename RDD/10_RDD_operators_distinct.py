from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 1, 1, 2, 2, 2, 3, 3, 3])
    print(rdd.distinct().collect())

    rdd = sc.parallelize([('a', 1), ('a', 2), ('a', 1), ('b', 1), ('b', 1), ('b', 3)])
    print(rdd.distinct().collect())