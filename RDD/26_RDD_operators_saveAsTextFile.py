from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    # rdd.saveAsTextFile("../Data/saveAsTextFile_3part")
    #
    # rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 1)
    # rdd.saveAsTextFile("../Data/saveAsTextFile_1part")

    rdd = sc.wholeTextFiles("../Data/saveAsTextFile_3part")
    print(rdd.collect())