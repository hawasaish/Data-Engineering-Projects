# Importing the SparkSession, functions and types from Pyspark's SQL module
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType
# Initializing the Spark session
# Providing a meaningful name to the application relevant to the steps performed in the application
# Set the log level to error
spark = SparkSession  \
	.builder  \
	.appName("StructuredClickstreamDatatoHadoop")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
# Reading the data from JSON file into a dataframe
df = spark.read.json('/user/hadoop/clickStreamData/*.json')
# Creating a new dataframe by extracting the columns from the JSON value
df1 = df.select(get_json_object(df['value'], "$.customer_id").alias("customer_id"),
               get_json_object(df['value'], "$.app_version").alias("app_version"),
               get_json_object(df['value'], "$.OS_version").alias("OS_version"),
               get_json_object(df['value'], "$.lat").alias("lat"),
               get_json_object(df['value'], "$.lon").alias("lon"),
               get_json_object(df['value'], "$.page_id").alias("page_id"),
               get_json_object(df['value'], "$.button_id").alias("button_id"),
               get_json_object(df['value'], "$.is_button_click").alias("is_button_click"),
               get_json_object(df['value'], "$.is_page_view").alias("is_page_view"),
               get_json_object(df['value'], "$.is_scroll_up").alias("is_scroll_up"),
               get_json_object(df['value'], "$.is_scroll_down").alias("is_scroll_down"),
               get_json_object(df['value'], "$.timestamp\n").alias("timestamp1"))

# Extracting only the required part from the timestamp1 column and saving in new column "timestamp"
df2 = df1.withColumn("timestamp", df1["timestamp1"][0:19])
# Creating a new dataframe by assigning the correct datatypes to the "customer_id", "lat" and "lon" columns
df3 = df2 \
  .withColumn("customer_id", df2["customer_id"].cast(IntegerType()))   \
  .withColumn("lat", df2["lat"].cast(FloatType()))    \
  .withColumn("lon", df2["lon"].cast(FloatType()))
# Selecting only the required columns as per the given schema
df4 = df3.select("customer_id","app_version","OS_version","lat","lon","page_id","button_id","is_button_click","is_page_view","is_scroll_up","is_scroll_down","timestamp")
# Printing the schema of the dataframe
print(df4.schema)
# Printing the first 10 records from the dataframe
df4.show(5)
# Printing the number of records
records = df4.count()
print("Number of records = " + str(records))
# Writing the structured clickstream data in CSV format to the hadoop location
df4.coalesce(1).write.format('csv').option('header','false').save('/user/hadoop/structuredclickStreamData', mode='overwrite')