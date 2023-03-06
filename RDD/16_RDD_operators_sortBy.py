from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('b', 3), ('c', 1), ('f', 5), ('a', 2),  ('j', 6), ('b', 4), ('b', 1)])

    # sortBy value
    # parameter1 is sortBy what
    # parameter2 is ascending or not
    # parameter2 is partition number
    # 如果需要全局有序，分区数需要设置为1

    print(rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1).collect())
    print(rdd.sortBy(lambda x: x[0], ascending=True, numPartitions=1).collect())





