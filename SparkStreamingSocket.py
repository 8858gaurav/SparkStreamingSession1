from pyspark.sql.functions import *
from pyspark.sql import SparkSession


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession.builder \
            .appName("streaming application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .master("local[2]") \
            .getOrCreate()
            # since the group by will create a 200 shuffle partitons.
            # to reduce the time, it's better to decrease the no of partitons, when 
            # we have a large no of micro batches with smalll-2 sizes. 
    
# 1. read the data

    lines = spark \
            .readStream \
            .format("Socket") \
            .option("host", "localhost") \
            .option("port", 9988) \
            .load()
    

# 2. prcoessing logic
    words = lines.select(explode(split(lines.value, " ")).alias("word"))
    wordsCounts = words.groupBy("word").count()
# 3. write the sink
    query = wordsCounts \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("checkpointLocation", "checkpointdir1") \
            .start()

    query.awaitTermination()