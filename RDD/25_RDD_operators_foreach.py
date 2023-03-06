from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 1)

    # foreach runs the same operation as map, but return nothing
    rdd.foreach(lambda x: x * 10)
    # use print to get the result
    rdd.foreach(lambda x: print(x*10))
