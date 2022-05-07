# Importing the SparkSession from Pyspark's SQL module
from pyspark.sql import SparkSession

# Initializing the Spark session
# Providing a meaningful name to the application relevant to the steps performed in the application
# Set the log level to error
spark = SparkSession  \
	.builder  \
	.appName("ClickstreamDatatoHadoop")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading the data from Kafka topic
# startingOffsets used as earliest to load all the data from the begininng
# Provided the given Kafka server along with port
# Provided the Kafka topic name from which the data has to be read
rawData = spark  \
	.readStream  \
	.format("kafka")  \
    .option("multiline","true")  \
    .option("startingOffsets", "earliest")  \
	.option("kafka.bootstrap.servers","##.###.###.###:####")  \
	.option("subscribe","de-capstone3")  \
	.load()
    
data = rawData.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Writing the raw data to the Haddop location in JSON format
# Provided target directory in Hadoop where the data will be stored
# Provided target directory in Hadoop where the data checkpoint will be created
finalClickStream = data  \
    .writeStream  \
    .format("json")  \
    .outputMode("append")  \
    .option("truncate", "false")  \
    .option('path', "/user/hadoop/clickStreamData")  \
    .option("checkpointLocation", "/user/hadoop/clickStreamDataCP") \
    .start()  \
    .awaitTermination()