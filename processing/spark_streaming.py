"""
=================================================================
Real-time Crypto Trading Advisor
Layer: Processing Engine
Component: PySpark Structured Streaming Job

Data Flow:
  Kafka (crypto_trades)
    ↓ [Bronze] Cast + clean → write Parquet to MinIO (partitioned)
    ↓ [Silver] 5-min SMA, Volume, Vwap → write to Cassandra
    ↓ [Gold]   RSI, alert flags → write to Cassandra (signals table)

Design patterns:
  - Watermark: 10 min late data tolerance
  - Checkpoint: S3A (MinIO) for fault tolerance
  - Foreachbatch sink: Cassandra writes (direct driver)
  - Window: 5-min tumbling + 1-min micro-batch slide
=================================================================
"""

import os # to getenv var in docker compose
import sys # to getenv var in docker compose
import logging # instead of use print
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, LongType,
)
from pyspark.sql.window import Window


#---------------------Logger config-----------------
logging.basicConfig( # overview rule for all logging
    level=logging.INFO, #  > INFO > WARNING > ERROR > CRITICAL
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)

logger = logging.getLogger("crypto.spark") # named logger for spark job

#---------------------Configuration---------------------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
TOPIC_TRADES = os.getenv("TOPIC_TRADES", "crypto_trades")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KS      = os.getenv("CASSANDRA_KS",     "crypto_advisor")
CHECKPOINT_BASE   = os.getenv("CHECKPOINT_DIR",   "s3a://crypto-checkpoints/spark")

#---------------------Data schema-----------------
TRADE_SCHEMA = StructType([
    StructField("trade_id",       StringType(),  False),
    StructField("symbol",         StringType(),  False),
    StructField("price",          DoubleType(),  False),
    StructField("quantity",       DoubleType(),  False),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("trade_time_ms",  LongType(),    False),
    StructField("event_time_ms",  LongType(),    True),
    StructField("ingest_time_ms", LongType(),    True),
    StructField("notional_usd",   DoubleType(),  True),
    StructField("latency_ms",     LongType(),    True),
    StructField("date_partition", StringType(),  True),

])

#--------------------- SparkSession (create Spark) -----------------



def create_spark() -> SparkSession:
    """
    Build SparkSession with:
      - Kafka connector
      - S3A (MinIO) connector for Parquet
      - Cassandra connector
    """
    return (
        SparkSession.builder
        .appName("CryptoTradingAdvisor")
        .master("spark://spark-master:7077")
        #-----------Cassandra--------------
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
        #----------- S3 minio -------------------
        .config("spark.hadoop.fs.s3a.endpoint",             MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",           MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",           MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        #----------- Streaming -----------------------
        .config("spark.sql.streaming.schemaInference",      "true")
        .config("spark.sql.shuffle.partitions", "4") # Spark chia dữ liệu thành 4 luồng song song (4 partitions) khi làm phép tính gộp,
        #------------JAR packages ---------------------
        .config( #spark need these packages to comunication between spark and Kafka and Cassandra
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
            
        )
        .getOrCreate() # Create or get the SparkSession
    )

#----------------------Layer1: get raw data from Kafka -----------------
"""
    Read from Kafka and deserialise JSON payload using declared schema.
    Apply watermark for late-data handling (10-minute tolerance).
"""

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    
    # read stream from kafka
    raw_df = (
        spark.readStream
        .format("kafka") # kafka source
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) # ip kafka server
        .option("subscribe",               TOPIC_TRADES) # topic name
        .option("startingOffsets",         "latest") # start from latest offset
        .option("maxOffsetsPerTrigger",    "5000")
        .option("failOnDataLoss",          "false")
        .load()     # execute to connect with kafka brokers
    )# -> raw_df -> bnary bytes

    # Transform  the raw data to DataFrame with schema 
    # raw_df: {key: null, value: binary, topic: "crypto_trades", partition: 1, offset: 101, timestamp: "2025-10-15T20:24:00.239Z", timestampType: 0}
    # value: b'{"trade_id":"123","symbol":"BTC/USDT","price":100000.0,"quantity":0.0001,"is_buyer_maker":true,"trade_time_ms":1765782240239}'
    trade_df = (
        raw_df
        .select(
            F.from_json( # cast from binary to json then extract to struct
                F.col("value").cast("string"),
                TRADE_SCHEMA
            ).alias("data") # alias => data is col
        )
        .select("data.*") # extract col from data
        .withColumn(
            "trade_timestamp",
            (F.col("trade_time_ms") / 1000).cast("timestamp")
        )
        .withWatermark("trade_timestamp", "10 minutes") # 10 min : độ trễ tối đa dữ liệu cho phép nhận
    
    )   

    return trade_df

#------------------- Layer2- Bronze (Cold storage)): Raw parquet (load data from Kafka and save to Minio in parquet format) ---------------
def write_bronze(trade_df: DataFrame) -> None:
    #    Sink raw (cleaned) trades to MinIO Bronze bucket as Parquet.
    #    Partitioned by date for efficient backtest queries.
    (
        trade_df
        .writeStream
        .format("parquet")
        .option("path",         "s3a://crypto-bronze/trades")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
        .partitionBy("date_partition", "symbol")
        .trigger(processingTime="30 seconds") # every 30s gom dữ liệu trong RAm -> ghi xuống MinIO 
        .outputMode("append") # append mode : 
        .start()
    )
    logger.info("✓ Bronze sink started → s3a://crypto-bronze/trades")


#--------------------Layer3 : Silver (Windowed Aggregations) :  ---------------------
def compute_silver(trade_df: DataFrame) -> DataFrame:
    """
    compute 5-minute tumbling window aggregations:
      - open, high, low, close (OHLC)
      - SMA-5 (avg price over 5-min window)
      - VWAP  (volume-weighted average price)
      - total_volume, total_notional
      - trade_count, avg_latency
    """
    return (
        trade_df 
        .groupBy(
            F.col("symbol"),
            F.window(F.col("trade_timestamp"), "5 minutes").alias("window")
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.avg("price").alias("sma_5min"),
            # VWAP = sum(price * quantity) / sum(quantity)
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
            F.sum("quantity").alias("total_volume"),
            F.sum("notional_usd").alias("total_notional_usd"),
            F.count("*").alias("trade_count"),
            F.avg("latency_ms").alias("avg_latency_ms"),
        )
        .select(
            F.col("symbol"),
            F.to_date("window.start").alias("date_bucket"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.round("sma_5min", 2).alias("sma_5min"),
            F.round("vwap", 2).alias("vwap"),
            F.round("total_volume", 6).alias("total_volume"),
            F.round("total_notional_usd", 2).alias("total_notional_usd"),
            F.col("trade_count"),
            F.round("avg_latency_ms", 1).alias("avg_latency_ms"),
        )
    )

"""Write 5-min OHLCV aggregations to Cassandra silver table."""
def write_silver(silver_df: DataFrame) -> None:

    def write_batch_silver(batch_df: DataFrame, batch_id: int) -> None: 
        count = batch_df.count() # get number of rows
        if count == 0: # Only write if there are rows
            return 
        #else if count > 0 
        logger.info(f"[Silver] batch_id={batch_id} rows={count}")
        # write to Cassandra with foreachbatch  
        (
            batch_df
            .write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="ohlcv_5min", keyspace=CASSANDRA_KS)
            .save()        
        )        

    (
        silver_df
        .writeStream
        .foreachBatch(write_batch_silver) 
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver")
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )
    logger.info("✓ Silver sink started → Cassandra:ohlcv_5min")

#đoạn tren sẽ là : writeStream -> mở ghi streaming
#+ foreachBatch -> call function ghi xuống cassasandra
#+ option -> save checkpoint 
#+ trigger : -> every 60s -> gom batch and call foreachBatch ghi xuống

#-> từ lúc spark chạy -> data gom đổ vào ram -> cứ 60s dc 1 batch trong ram đó


#-------------------Layer 4: Gold  (Signal generation -> Cassandra ---------------------
def compute_gold(trade_df: DataFrame) -> DataFrame: 
    
    base_df = (
        trade_df  
        .groupBy(
            F.col("symbol"),
            F.window(F.col("trade_timestamp"), "15 minutes").alias("window")
        )
        .agg(
            F.last("price").alias("close"),
            F.avg("price").alias("sma_15min"),
            F.sum("quantity").alias("volume_15min"),
            F.sum(
                F.when(F.col("is_buyer_maker") == False, F.col("quantity")).otherwise(0)
            ).alias("buy_volume"),
            F.sum(
                F.when(F.col("is_buyer_maker") == True,  F.col("quantity")).otherwise(0)
            ).alias("sell_volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            "symbol",
            F.to_date("window.start").alias("date_bucket"),
            F.col("window.start").alias("window_start"),
            "close", "sma_15min", "volume_15min",
            "buy_volume", "sell_volume", "trade_count",
        )
    )

    # Add signal flag derivation
    gold_df = base_df.withColumn(
        # Volume spike: sell volume dominates AND volume is high
        "volume_spike",
        F.when(
            (F.col("sell_volume") > F.col("buy_volume") * 1.5),
            True
        ).otherwise(False)
    ).withColumn(
        # Price deviation from SMA
        "price_deviation_pct",
        F.round(
            F.abs(F.col("close") - F.col("sma_15min")) / F.col("sma_15min") * 100,
            4
        )
    ).withColumn(
        # Buy pressure ratio (0 = all sells, 1 = all buys)
        "buy_pressure",
        F.round(
            F.col("buy_volume") / (F.col("buy_volume") + F.col("sell_volume")),
            4
        )
    ).withColumn(
        # Composite risk level
        "risk_level",
        F.when(
            (F.col("volume_spike") == True) & (F.col("price_deviation_pct") > 2.0),
            "CRITICAL"
        ).when(
            (F.col("volume_spike") == True) | (F.col("price_deviation_pct") > 2.0),
            "HIGH"
        ).when(
            F.col("price_deviation_pct") > 1.0,
            "MEDIUM"
        ).otherwise("LOW")
    ).withColumn(
        # Human-readable alert message
        "alert_message",
        F.when(
            F.col("risk_level") == "CRITICAL",
            F.concat_ws(" | ",
                F.lit("⚠️ CRITICAL RISK"),
                F.concat(F.lit("Price dev: "), F.col("price_deviation_pct").cast("string"), F.lit("%")),
                F.lit("Volume dump detected"),
            )
        ).when(
            F.col("risk_level") == "HIGH",
            F.concat(F.lit("🔴 HIGH risk - Monitor closely. Deviation: "),
                     F.col("price_deviation_pct").cast("string"), F.lit("%"))
        ).otherwise(F.lit(None).cast("string"))
    )
    
    return gold_df

def write_gold(gold_df: DataFrame) -> None:
    
    def write_batch_gold(batch_df: DataFrame, batch_id: int):
        count = batch_df.count()
        if count == 0:
            return 
        critical = batch_df.filter(F.col("risk_level").isin("HIGH", "CRITICAL")).count()
        logger.info(f"[Gold] batch_id={batch_id} rows={count} high_risk={critical}")
        (
            batch_df
            .write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="trading_signals", keyspace=CASSANDRA_KS)
            .save()
        )

    (
        gold_df
        .writeStream
        .foreachBatch(write_batch_gold)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold")
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )
    logger.info("✓ Gold sink started → Cassandra:trading_signals")


# -----------------Main Job---------------------
def main() -> None:
    logger.info("=" * 60)
    logger.info(" Crypto Trading Advisor — Spark Streaming Engine")
    logger.info(f" Kafka:     {KAFKA_BROKERS} | Topic: {TOPIC_TRADES}")
    logger.info(f" MinIO:     {MINIO_ENDPOINT}")
    logger.info(f" Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
    logger.info("=" * 60)

    # create spark 
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    #get source from kafka 
    trade_df = read_kafka_stream(spark)  

    # cold storage -> Bronze
    write_bronze(trade_df)

    # silver transform and store in cassandra
    silver_df = compute_silver(trade_df) # 1min OHLC
    write_silver(silver_df) # 1min OHLC 

    #gold transform 
    gold_df = compute_gold(trade_df)
    write_gold(gold_df)

    # Block main threads here and keep job alive
    spark.streams.awaitAnyTermination() # Block until manually stopped 

if __name__ == "__main__":
    main()

# run docker-compose again  x
# create storage and cassandra-init.cql x

# tạo topic cho  kafka
""" crypto_trade
docker exec -it kafka kafka-topics --create --topic crypto_trades --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
"""
"""DLQ
docker exec DLQ-it kafka kafka-topics --create --topic crypto_trades_dlq --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

"""
# run file spark 
"""
docker exec -it crypto-spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /opt/bitnami/spark/jobs/spark_streaming.py

"""

