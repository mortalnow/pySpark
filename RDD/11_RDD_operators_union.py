from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3, 4, 5])
    rdd2 = sc.parallelize(['a', 'c', 'g', 'c', 'l', 'o', 't'])

    rdd3 = rdd1.union(rdd2)
    print(rdd3.collect())

    # rdd could union different data type