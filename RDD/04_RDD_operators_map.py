from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

    # define a function as input of a map
    def add(data):
        return data * 10

    print(rdd.map(add).collect())

    # easy way, use lambda
    print(rdd.map(lambda data: data*10).collect())