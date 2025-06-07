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
    completed_orders = spark.sql("select * from orders where order_status = 'COMPLETE'")

# 3. write the sink
    query = completed_orders \
            .writeStream \
            .format("csv") \
            .outputMode("append") \
            .option("path","/Users/gauravmishra/Desktop/UnboundedStreaming/outputdir") \
            .option("checkpointLocation", "checkpointdir1") \
            .start()

    query.awaitTermination()