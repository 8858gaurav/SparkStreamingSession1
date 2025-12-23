from pyspark.sql.functions import *
from pyspark.sql import SparkSession


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession.builder \
            .appName("streaming application") \
            .config("spark.driver.bindAddress", 'localhost') \
            .config("spark.ui.port", "4050") \
            .config("spark.driver.port", "4051") \
            .config("spark.sql.shuffle.partitions", 3) \
            .master("local[2]") \
            .getOrCreate()
            # since the group by will create a 200 shuffle partitons.
            # to reduce the time, it's better to decrease the no of partitons, when 
            # we have a large no of micro batches with smalll-2 sizes. 

            # Now adding the file 1 by 1 from temp folder to input dir
    
# 1. read the data
    order_schema = 'order_id long, order_date date, order_customer_id long, order_status string'
    orders_df = spark \
            .readStream \
            .format("json") \
            .schema(order_schema) \
            .option("path", "/Users/gauravmishra/Desktop/SparkStreamingSession1/UnboundedStreaming/inputdir") \
            .load()
    

# 2. prcoessing logic
    orders_df.createOrReplaceTempView("orders")
    agg_orders = spark.sql("select order_status, count(*) as total from orders group by 1")

# 3. write the sink
# in this once we placed the file1.json, and then file2.json, you will see, it will accumulated from the previouc batch output
# do the same thing with the append mode, update mode
# delete the check point directory before running this script
    query = agg_orders \
            .writeStream \
            .format("console") \
            .outputMode("complete") \
            .option("checkpointLocation", "checkpointdir1") \
            .start()

    query.awaitTermination()

# If you try to use Complete Mode while writing to Parquet or CSV file sinks in Spark Structured Streaming, the job will fail with an error.
# Why doesn't it work?
# Complete Mode is designed to output the entire result table every time a new micro-batch is processed.
# File Sinks (Parquet/CSV): These are "append-only" by nature. To support Complete Mode, Spark would have to delete all existing files and rewrite the entire dataset into the folder every few seconds.
# Safety: Spark avoids this because it would lead to massive data duplication and performance collapse as the dataset grows.

# If you are writing raw data or transformed rows (without aggregations), use Append Mode. 
# This is the native way Parquet and CSV sinks work; Spark simply adds new files to the directory for every batch.

# {"order_id": 1, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 11599, "order_status": "CLOSED"}
# {"order_id": 2, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 256, "order_status": "PENDING_PAYMENT"}
# {"order_id": 3, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 12111, "order_status": "COMPLETE"}
# {"order_id": 4, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 8827, "order_status": "CLOSED"}

# Batch: 0
# -------------------------------------------
# +---------------+-----+
# |   order_status|total|
# +---------------+-----+
# |         CLOSED|    2|
# |PENDING_PAYMENT|    1|
# |       COMPLETE|    1|
# +---------------+-----+

# {"order_id": 5, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 11318, "order_status": "COMPLETE"}
# {"order_id": 6, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 7130, "order_status": "COMPLETE"}
# {"order_id": 7, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 4530, "order_status": "COMPLETE"}
# {"order_id": 8, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 2911, "order_status": "PROCESSING"}
# {"order_id": 9, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 5657, "order_status": "PENDING_PAYMENT"}
# {"order_id": 10, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 5648, "order_status": "PENDING_PAYMENT"}

# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +---------------+-----+
# |   order_status|total|
# +---------------+-----+
# |         CLOSED|    2|
# |PENDING_PAYMENT|    3|
# |       COMPLETE|    4|
# |     PROCESSING|    1|
# +---------------+-----+

# {"order_id": 5, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 11318, "order_status": "COMPLETE"}
# {"order_id": 6, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 7130, "order_status": "COMPLETE"}
# {"order_id": 7, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 4530, "order_status": "COMPLETE"}
# {"order_id": 8, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 2911, "order_status": "PROCESSING"}
# {"order_id": 9, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 5657, "order_status": "PROCESSING"}
# {"order_id": 10, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 5648, "order_status": "PENDING_PAYMENT"}

# -------------------------------------------
# Batch: 2
# -------------------------------------------
# +---------------+-----+
# |   order_status|total|
# +---------------+-----+
# |         CLOSED|    2|
# |PENDING_PAYMENT|    4|
# |     PROCESSING|    3|
# |       COMPLETE|    7|
# +---------------+-----+
