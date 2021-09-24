from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

#  A StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic

redisServerSchema= StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", BooleanType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

#  A StructType for the Customer JSON that comes from Redis

customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# A StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis-
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])
# A spark application object
spark = SparkSession.builder.appName("JOINT REDIS STEDI-EVENT APPLICATION").getOrCreate()

# Setting the spark log level to WARN

spark.sparkContext.setLogLevel("WARN")

# Using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# The option ("startingOffsets","earliest")  is used to read all the events from the topic including those that were published before starting the spark stream

redisStediDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","redis-server").option("startingOffsets","earliest").load()   
#  Casting the value column in the streaming dataframe as a STRING 
redisStediDF = redisStediDF.selectExpr( "cast(value as string) value")

# Parsing the single column "value" with a json object in it
# storing them in a temporary view called RedisSortedSet
redisStediDF.withColumn("value", from_json("value", redisServerSchema)).select(col('value.*')).createOrReplaceTempView("RedisSortedSet")



# executing a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and creating a column called encodedCustomer

encodedRedisDF = spark.sql("""
SELECT zSetEntries[0].element AS encodedCustomer
FROM RedisSortedSet
""")

#  base64 decoding the redisEvent
decodedRedisDF = encodedRedisDF.withColumn("customer",unbase64(encodedRedisDF.encodedCustomer).cast("string"))



#  Parsing the JSON in the Customer record and store in a temporary view called CustomerRecords


decodedRedisDF.withColumn("customer", from_json("customer", customerSchema)).select(col('customer.*')).createOrReplaceTempView("CustomerRecords")

          
#  JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF

emailAndBirthDayStreamingDF =  spark.sql("""
        SELECT email, birthDay 
        FROM CustomerRecords
        WHERE email IS NOT NULL AND birthDay IS NOT NULL 
""")
                         
# Selecting the email and the birth year from the emailAndBirthDayStreamingDF dataframe  (using the split function)

#  Selecting only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
                         

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email',split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))
                         

stediEventsDF  = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","stedi-events").option("startingOffsets","earliest").load()      

# cast the value column in the streaming dataframe as a STRING 

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
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

#  parsing the JSON from the single column "value" with a json object in it
# and storing it in a temporary view called CustomerRisk

stediEventsDF.withColumn("value", from_json("value", stediEventsSchema)).select(col('value.customer'), col('value.score'), col('value.riskDate')).createOrReplaceTempView("CustomerRisk")

# Executing a sql statement against the temporary view CustomerRisk, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF

customerRiskStreamingDF = spark.sql("""
    SELECT customer, score 
    FROM CustomerRisk
""")


# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe

redisStediScoreJointStreamingDF = customerRiskStreamingDF.join(emailAndBirthYearStreamingDF, expr("customer = email"))                         
print("emailAndBirthDayStreamingDF output:")

#emailAndBirthDayStreamingDF.writeStream\
 #       .format("console")\
  #      .outputMode("append")\
   #     .start()\
    #    .awaitTermination()
                         
    
print("emailAndBirthYearStreamingDF output:")

# emailAndBirthDayStreamingDF.writeStream\
        #.format("console")\
       # .outputMode("append")\
      #  .start()\
     #   .awaitTermination()
                         
print("customerRiskStreamingDF output:")

#customerRiskStreamingDF.writeStream\
 #       .format("console")\
  #      .outputMode("append")\
   #     .start()\
    #    .awaitTermination()

print("emailAndBirthDayStreamingDF output:")

#emailAndBirthDayStreamingDF.writeStream\
    #    .format("console")\
     #   .outputMode("append")\
      #  .start()\
       # .awaitTermination()
                         


# Sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
redisStediScoreJointStreamingDF.selectExpr(
  "cast(customer as string) as key",
  "to_json(struct(*)) as value").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "risk-output-stedi-event-score").option("checkpointLocation", "/tmp/kafkacheckpoint").start().awaitTermination()