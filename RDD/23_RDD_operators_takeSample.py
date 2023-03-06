from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 100))
    print(rdd.takeSample(True, 5, seed=5))
    print(rdd.takeSample(True, 5))