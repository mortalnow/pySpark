# import spark related packages
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # Initialize the execution environment, build SparkContext
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # textFile API read data
    file_rdd1 = sc.textFile("../data/words.txt")
    print("Default partitions number is", file_rdd1.getNumPartitions())
    print("file_rdd1 data is ", file_rdd1.collect())

    # test partition number setting
    file_rdd2 = sc.textFile("../data/words.txt", 3)
    file_rdd3 = sc.textFile("../data/words.txt", 100)
    print("Partitions number is", file_rdd2.getNumPartitions())
    print("Partitions number is", file_rdd3.getNumPartitions())
    # spark will define partition number itself, the setting is just a reference

    # read file from HDFS
    hdfs_rdd = sc.textFile("hdfs://node1:8020/words.txt")
    print("hdfs_rdd data is ", hdfs_rdd.collect())



