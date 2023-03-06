from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("new").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # read txt to create rdd
    file_rdd = sc.textFile('../Data/words.txt')

    # split the lines to words by flatMap
    word_rdd = file_rdd.flatMap(lambda x: x.split(" "))

    # use map to get multiple (word:1), key:value pair
    word_with_one_rdd = word_rdd.map(lambda word: (word, 1))

    # use reduceByKey to add all values with same key
    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # collect and print
    print(result_rdd.collect())

