import time

from pyspark import SparkConf, SparkContext
from pyspark import StorageLevel

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # set the check point directory
    sc.setCheckpointDir("hdfs://node1:8020/output/ckp")
    rdd1 = sc.textFile('../Data/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))

    # store rdd3 in checkpoint
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    # relieve rdd3 cache
    rdd3.unpersist()
