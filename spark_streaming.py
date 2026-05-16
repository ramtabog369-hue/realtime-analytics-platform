from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("ClickstreamAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", "clickhouse-jdbc-0.6.0-all.jar") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("action", StringType()),
    StructField("price", FloatType()),
    StructField("timestamp", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clicks") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed = parsed.withColumn("event_time", col("timestamp").cast(TimestampType()))

aggregates = parsed.groupBy(
    window(col("event_time"), "1 minute"),
    col("product_id"),
    col("action")
).agg(
    count("*").alias("event_count"),
    _sum("price").alias("total_price")
).select(
    col("window.start").alias("window_start"),
    col("product_id"),
    col("action"),
    col("event_count"),
    col("total_price")
)

def write_to_clickhouse(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://localhost:8123/analytics") \
        .option("dbtable", "product_aggregates") \
        .option("user", "admin") \
        .option("password", "admin123") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

query1 = aggregates.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query2 = parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta_checkpoint") \
    .trigger(processingTime="30 seconds") \
    .start("s3a://datalake/clickstream/")

print("Spark Streaming запущен!")
spark.streams.awaitAnyTermination()