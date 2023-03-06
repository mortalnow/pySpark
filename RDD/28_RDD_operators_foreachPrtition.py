from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 4, 53, 5, 54, 545, 23], 3)

    def process(iter):
        result = list()
        for i in iter:
            result.append(i * 10)
        # if not print, nothing return
        print(result)

    rdd.foreachPartition(process)
