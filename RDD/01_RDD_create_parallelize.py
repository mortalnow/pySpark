
# import spark related packages
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # Initialize the execution environment, build SparkContext
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # get default partition number
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    print("Default partition number is ", rdd.getNumPartitions())

    # try to set partition number
    rdd = sc.parallelize([1, 2, 3], 3)
    print("Partition number is ", rdd.getNumPartitions())

    # collect will send all data from each partiton to Driver and form a List
    print(rdd.collect())