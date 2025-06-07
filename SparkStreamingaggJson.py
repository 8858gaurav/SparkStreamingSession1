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
    order_schema = 'order_id long, order_date date, order_customer_id long, order_status string'
    orders_df = spark \
            .readStream \
            .format("json") \
            .schema(order_schema) \
            .option("path", "/Users/gauravmishra/Desktop/UnboundedStreaming/inputdir") \
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