from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hive', 1), ('hadoop', 1), ('spark', 1), ('happy', 4)])


    def process(k):
        if 'hadoop' == k or 'spark' == k: return 0
        if 'hive' == k: return 1
        return 2

    print(rdd.partitionBy(3, process).glom().collect())
