from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(1001, "zhangsan"), (1002, "List"), (1003, "wangwu"), (1004, "zhaoliu")])
    rdd2 = sc.parallelize([(1001, "Sales"), (1002, "Tech Guy"), (1006, "Dancer")])

    # only can join key:value pair
    # by default, only can join by key
    print(rdd1.join(rdd2).collect())
    print(rdd1.leftOuterJoin(rdd2).collect())
    print(rdd1.rightOuterJoin(rdd2).collect())