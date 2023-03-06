from pyspark import SparkConf, SparkContext
import re

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile("../Data/accumulator_broadcast_data.txt")

    abnormal_list = [",", ".", "!", "#", "$", "%"]

    # broadcast abnormal_list
    broadcast = sc.broadcast(abnormal_list)
    # set accumulator
    acmlt = sc.accumulator(0)

    # remove empty line
    line_rdd = file_rdd.filter(lambda line: line.trip())

    # remove space
    data_rdd = file_rdd.map(lambda line: line.trip())

    # by \s+ to remove not sure number space
    words_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))

    def filter_func(data):
        global acmlt
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            acmlt += 1
            return False
        else:
            return True

    normal_words_rdd = words_rdd.filter(filter_func)

    result_rdd = normal_words_rdd.map(lambda x: (x, 1)).\
        reduceByKey(lambda a, b: a+b)

    print(result_rdd.collect())
    print(acmlt)
