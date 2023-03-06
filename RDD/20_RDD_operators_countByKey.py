from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile("../Data/words.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))

    result = rdd2.countByKey()

    print(result)
    print(type(result))
