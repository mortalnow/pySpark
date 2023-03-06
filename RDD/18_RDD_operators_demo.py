#coding:utf8

from pyspark import SparkConf, SparkContext
import json

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # read file
    file_rdd = sc.textFile("../Data/order.text")

    # split by | and un-nesting
    jsons_rdd = file_rdd.flatMap(lambda line: line.split("|"))

    # read by default json loader: json to dict
    dict_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))

    # filter 北京 area
    beijing_rdd = dict_rdd.filter(lambda d: d["areaName"] == "北京")

    # get only areaName and category
    category_rdd = beijing_rdd.map(lambda d: d["areaName"] + "_" + d["category"])

    # remove duplicates
    result = category_rdd.distinct()

    # collect to print result
    print(result.collect())








