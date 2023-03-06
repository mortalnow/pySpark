from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('b', 3), ('c', 1), ('f', 5), ('A', 2),  ('J', 6), ('B', 4), ('b', 1)], 3)

    # not remove influence of upper and lower case
    print(rdd.sortByKey(ascending=True, numPartitions=1).collect())
    # remove influence of upper and lower case
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())






