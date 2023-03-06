from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    # set accumulator at 0
    acmlt = sc.accumulator(0)

    def map_func(data):
        global acmlt
        acmlt += 1

    rdd2 = rdd.map(map_func)
    # cache rdd2 incase re-run the whole rdd chain and get higher number(20 in this case)
    # try to comment the rdd2.cache(), you'll get 20 at the end
    rdd2.cache()
    rdd2.collect()

    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    print(acmlt)
