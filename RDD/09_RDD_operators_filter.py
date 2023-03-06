from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6])

    # filter to get odd number
    result = rdd.filter(lambda x: x%2 == 1)
    print(result.collect())

    # filter to get even number
    result = rdd.filter(lambda x: x % 2 == 0)
    print(result.collect())