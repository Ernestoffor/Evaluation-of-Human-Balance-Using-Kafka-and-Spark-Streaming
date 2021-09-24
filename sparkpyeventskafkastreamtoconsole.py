from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# a spark application object
spark = SparkSession.builder.appName("STEDI EVENTS APPLICATION")\
.getOrCreate()

# setting the spark log level to WARN

spark.sparkContext.setLogLevel("WARN")

# Using the spark application object, read a streaming dataframe from the Kafka topic 'stedi-event' as the source


stediEventsDF  = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load()      

# casting the value column in the streaming dataframe as a STRING 

stediEventsDF = stediEventsDF .selectExpr( "cast(value as string) value")
            

#  A StructType Schema for the Customer JSON that comes from Redis

customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# A StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis-
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", StringType())
])

#  parse the JSON from the single column "value" with a json object in it
# and storing it in a temporary view called CustomerRisk

stediEventsDF.withColumn("value", from_json("value", stediEventsSchema))\
.select(col('value.customer'), col('value.score'), col('value.riskDate'))\
.createOrReplaceTempView("CustomerRisk")

# Executing a sql statement against the temporary view CustomerRisk, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF

customerRiskStreamingDF = spark.sql("""
    SELECT customer, score 
    FROM CustomerRisk
""")
# sinking the customerRiskStreamingDF dataframe to the console in append mode
customerRiskStreamingDF.writeStream\
        .format("console")\
        .outputMode("append")\
        .start()\
        .awaitTermination()
                         

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 