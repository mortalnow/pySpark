from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('a', 1), ('b', 1), ('b', 1)])

    # use groupBy to group
    # pass in the parameter to be groupBy
    result = rdd.groupBy(lambda t: t[0])
    
    print(result.collect())
    # the value of the result is a list, we transform it by list() as below
    print(result.map(lambda t:(t[0], list(t[1]))).collect())
