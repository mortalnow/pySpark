from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == "__main__":
    # build SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # get sparkContext by SparkSession
    sc = spark.sparkContext

    pdf = pd.DataFrame({"id": [1, 2, 3],
                        "name": ["张大仙","王晓晓","吕不为"],
                        "age": [99, 11, 109]
                        })

    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()



