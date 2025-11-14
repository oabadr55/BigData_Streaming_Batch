from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# --- Create Spark Session ---
spark = (
    SparkSession.builder
    .appName("KafkaConsumer")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .getOrCreate()
)

# --- Define full schema of the JSON coming from Kafka ---
data_schema = StructType([
    StructField("Unnamed: 0.1", IntegerType()),
    StructField("Unnamed: 0", IntegerType()),
    StructField("index", IntegerType()),
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", FloatType()),
    StructField("trip_distance", FloatType()),
    StructField("RatecodeID", FloatType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", FloatType()),
    StructField("extra", FloatType()),
    StructField("mta_tax", FloatType()),
    StructField("tip_amount", FloatType()),
    StructField("tolls_amount", FloatType()),
    StructField("improvement_surcharge", FloatType()),
    StructField("total_amount", FloatType()),
    StructField("congestion_surcharge", FloatType()),
    StructField("airport_fee", FloatType()),
    StructField("trip_duraion", FloatType()),
    StructField("Plongitude", FloatType()),
    StructField("Platitude", FloatType()),
    StructField("Dlongitude", FloatType()),
    StructField("Dlatitude", FloatType()),
    StructField("pickup_date", StringType()),
    StructField("st_abb", StringType()),
    StructField("st_code", IntegerType()),
    StructField("date", StringType()),
    StructField("stability", StringType()),
    StructField("tmin", FloatType()),
    StructField("tmax", FloatType()),
    StructField("tavg", FloatType()),
    StructField("ppt", FloatType())
])

# --- Define the outer schema for Kafka messages ---
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("data", data_schema),
    StructField("chunk_id", IntegerType())
])

# --- Read stream from Kafka ---
df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "nyc_taxi_trips")
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "/tmp/spark_checkpoint")
        .load()
)

# --- Parse JSON ---
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df_parsed = df_parsed.select(from_json(col("json_str"), schema).alias("root"))

# --- Flatten nested structure (data.*) ---
df_expanded = df_parsed.select(
    col("root.timestamp"),
    col("root.chunk_id"),
    col("root.data.*")
)

# --- Add column based on temperature ---
df_with_band = df_expanded.withColumn(
    "Is_cold",
    when(col("tmax") >= 25, "Hot").otherwise("Cold")
)


# --- Write stream to console ---
query = (
    df_with_band.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "/tmp/spark_checkpoint")
        .start()
)

query.awaitTermination()
