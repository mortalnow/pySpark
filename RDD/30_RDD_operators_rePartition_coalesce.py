from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

    # rePartition to change partition: add or minus
    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())

    # coalesce only can minus partition by default, but can be done by set shuffle = True
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5).getNumPartitions())
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())