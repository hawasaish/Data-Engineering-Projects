# System dependencies for CDH
import os
import sys

# Importing required functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Utility function for calculating the total number of items in a single transaction
def get_item_count(items):
    item_count = 0
    for item in items:
        item_count = item_count + item["quantity"]
    return item_count

# Utility function for calculating the total cost for single transaction
def get_total_cost(type, items):
    total_cost = 0
    if type == "ORDER":
        for item in items:
            total_cost = total_cost + (item["quantity"] * item["unit_price"])
    else:
        for item in items:
            total_cost = total_cost - (item["quantity"] * item["unit_price"])
    return total_cost

# Utility function for determining if transaction is of type ORDER
def get_is_order(type):
    is_order = 0
    if type == "ORDER":
        is_order = 1
    else:
        is_order = 0
    return is_order

# Utility function for determining if transaction is of type RETURN
def get_is_return(type):
    is_return = 0
    if type == "RETURN":
        is_return = 1
    else:
        is_return = 0
    return is_return

# Initializing Spark session
spark = SparkSession  \
	.builder  \
	.appName("RetailDataAnalysis")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	

# Reading data from Kafka topic
rawData = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","###########:####")  \
    .option("startingOffsets", "latest")  \
	.option("subscribe","real-time-project")  \
	.load()

# Defining schema for a single order
orderSchema = StructType()  \
    .add("invoice_no", StringType())  \
    .add("country", StringType())  \
    .add("timestamp", TimestampType())  \
    .add("type", StringType())  \
    .add("items", ArrayType(StructType([
    StructField("SKU", StringType()),
    StructField("title", StringType()),
    StructField("unit_price", FloatType()),
    StructField("quantity", IntegerType()),
])))


orderStream = rawData.select(from_json(col("value").cast("string"), orderSchema).alias("data")).select("data.*")

# Defining the UDFs for the utility functions.
totalItemCount = udf(get_item_count, IntegerType())
totalCost = udf(get_total_cost, FloatType())
isOrder = udf(get_is_order, IntegerType())
isReturn = udf(get_is_return, IntegerType())

# Adding the calculated UDF columns
finalOrderStream = orderStream  \
    .withColumn("total_items", totalItemCount(orderStream.items))  \
    .withColumn("total_cost", totalCost(orderStream.type, orderStream.items))  \
    .withColumn("is_order", isOrder(orderStream.type))  \
    .withColumn("is_return", isReturn(orderStream.type))
    
# Writing the summarised table values to the console
finalOrderQuery = finalOrderStream  \
    .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return")  \
    .writeStream  \
    .outputMode("append")  \
    .format("console")  \
    .option("truncate", "false")  \
    .trigger(processingTime="1 minute")  \
    .start()

# Calculating the time based KPIs
aggStreamByTime = finalOrderStream  \
    .withWatermark("timestamp", "1 minute")  \
    .groupBy(window("timestamp", "1 minute", "1 minute"))  \
    .agg(sum("total_cost"), count("invoice_no").alias("OPM"), avg("total_cost"), avg("is_return"))  \
    .select("window", "OPM", format_number("sum(total_cost)", 2).alias("total_sale_volume"), format_number("avg(total_cost)", 2).alias("average_transaction_size"), format_number("avg(is_return)", 2).alias("rate_of_return"))

# Writing time based KPIs to json file
queryByTime = aggStreamByTime  \
    .writeStream  \
    .format("json")  \
    .outputMode("append")  \
    .option("truncate", "false")  \
    .option("path", "/tmp/KPIsByTime")  \
    .option("checkpointLocation", "/tmp/cpTime") \
    .trigger(processingTime="1 minute")  \
    .start()

# Calculating the time and country based KPIs
aggStreamByTimeCountry = finalOrderStream  \
    .withWatermark("timestamp", "1 minute")  \
    .groupBy(window("timestamp", "1 minute", "1 minute"), "country")  \
    .agg(sum("total_cost"), count("invoice_no").alias("OPM"), avg("is_return"))  \
    .select("window", "country", "OPM", format_number("sum(total_cost)", 2).alias("total_sale_volume"), format_number("avg(is_return)", 2).alias("rate_of_return"))

# Writing time and country based KPIs to json file
queryByTimeCountry = aggStreamByTimeCountry  \
    .writeStream  \
    .format("json")  \
    .outputMode("append")  \
    .option("truncate", "false")  \
    .option("path", "/tmp/KPIsByTimeCountry")  \
    .option("checkpointLocation", "/tmp/cpTimeCountry") \
    .trigger(processingTime="1 minute")  \
    .start()
    
queryByTimeCountry.awaitTermination()