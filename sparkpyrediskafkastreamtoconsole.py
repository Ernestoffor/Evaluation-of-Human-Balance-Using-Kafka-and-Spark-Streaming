from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

#  A StructType for the Kafka redis-server topic 
redisServerSchema= StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
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
    StructField("score", StringType()),
    StructField("riskDate", StringType())
])

#  a spark application object
spark = SparkSession.builder.appName("STEDI APPLICATION")\
.getOrCreate()

# setting the spark log level to WARN

spark.sparkContext.setLogLevel("WARN")

# Using the spark application object to read a streaming dataframe from the Kafka topic redis-server as the source
# The option ("startingOffsets","earliest")  is used to read all the events from the topic including those that were published before starting the spark stream

redisStediDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","redis-server").option("startingOffsets","earliest").load()      

# casting the value column in the streaming dataframe as a STRING 

redisStediDF = redisStediDF.selectExpr( "cast(value as string) value")

# Parsing the single column "value" with a json object in it and
# storing it in a temporary view called RedisSortedSet


redisStediDF.withColumn("value", from_json("value", redisServerSchema))\
.select(col('value.*'))\
.createOrReplaceTempView("RedisSortedSet")

# executing a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and creating a column called encodedCustomer



encodedRedisDF = spark.sql("""
SELECT zSetEntries[0].element AS encodedCustomer
FROM RedisSortedSet
""")

#  base64 decoding the redisEvent
decodedRedisDF = encodedRedisDF.withColumn("customer",unbase64(encodedRedisDF.encodedCustomer).cast("string"))



#  parsing the JSON in the Customer record and store in a temporary view called CustomerRecords


decodedRedisDF.withColumn("customer", from_json("customer", customerSchema)).select(col('customer.*')).createOrReplaceTempView("CustomerRecords")

#  JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF

emailAndBirthDayStreamingDF =  spark.sql("""
        SELECT email, birthDay 
        FROM CustomerRecords
        WHERE email IS NOT NULL AND birthDay IS NOT NULL 
""")
                         
# selecting the email and the birth year from the emailAndBirthDayStreamingDF dataframe  (using the split function)
#  Selecting only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
                         

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email',split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))
                         

# sinking the emailAndBirthYearStreamingDF dataframe to the console in append mode

emailAndBirthYearStreamingDF.writeStream.format("console").outputMode("append").start().awaitTermination()
                         


# Run the python script by running the following command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct 