from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 5), ('a', 1), ('b', 3), ('b', 2), ('b', 5), ])

    # only apply on Key:Value
    rdd2 = rdd1.groupByKey()
    print(rdd2.collect())
    print(rdd2.map(lambda a: (a[0], list(a[1]))).collect())

    rdd3 = rdd1.reduceByKey(lambda a, b: a + b)
    print(rdd3.collect())
