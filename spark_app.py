import os
from pathlib import Path

# --- Clear problematic auth-related env vars (IBM/JAAS) ---
for k in [
    "JAVA_SECURITY_AUTH_LOGIN_CONFIG",
    "HADOOP_JAAS_CONF",
    "JAAS_CONFIG",
    "HADOOP_OPTS",
    "JAVA_TOOL_OPTIONS",
    "SPARK_DIST_CLASSPATH",
    "CLASSPATH",
]:
    os.environ.pop(k, None)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

# ---------- Paths ----------
BASE_DIR = Path(__file__).parent.resolve()
INPUT_DIR = BASE_DIR / "data" / "input" / "json_stream"
OUTPUT_DIR = BASE_DIR / "data" / "output"
CHECKPOINT_DIR = BASE_DIR / "data" / "checkpoints"

CRITICAL_PATH = OUTPUT_DIR / "critical"
AVG_PATH = OUTPUT_DIR / "avg"
HUMIDITY_PATH = OUTPUT_DIR / "humidity"
BASELINE_PATH = OUTPUT_DIR / "baselines"

for p in [INPUT_DIR, CRITICAL_PATH, AVG_PATH, HUMIDITY_PATH, BASELINE_PATH, CHECKPOINT_DIR]:
    os.makedirs(p, exist_ok=True)

USE_MOCK = os.getenv("USE_MOCK_SOURCE", "0") == "1"

# ---------- Spark Session ----------
spark = (
    SparkSession.builder
    .appName("National Weather Service Live Observations")
    .master("local[*]")
    # force simple (non-kerberos) auth
    .config("spark.hadoop.security.authentication", "simple")
    .config("spark.hadoop.security.authorization", "false")
    .config("spark.authenticate", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# ---------- Source ----------
if USE_MOCK:
    sensor_data = (
        spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        .withColumn("station_id", F.expr("CAST(value % 10 AS STRING)"))
        .withColumn(
            "temperature",
            F.expr("CASE WHEN value % 10 = 0 THEN 10 + rand() * 5 ELSE 20 + rand() * 20 END"),
        )
        .withColumn("humidity", F.expr("35 + rand() * 40"))
        .withColumn("timestamp", F.col("timestamp"))
        .withColumn("latitude", F.expr("37 + (value % 5)") )
        .withColumn("longitude", F.expr("-122 + (value % 5)") )
    )
else:
    schema = StructType(
        [
            StructField("station_id", StringType(), False),
            StructField("temperature", DoubleType(), False),
            StructField("humidity", DoubleType(), False),
            StructField("timestamp", StringType(), False),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )
    sensor_data = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 4)
        .json(str(INPUT_DIR))
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .filter(F.col("timestamp").isNotNull())
    )

# ---------- Queries ----------
heat_index_expr = (
    F.lit(-8.784695)
    + F.lit(1.61139411) * F.col("temperature")
    + F.lit(2.338549) * F.col("humidity")
    - F.lit(0.14611605) * F.col("temperature") * F.col("humidity")
    - F.lit(0.012308094) * F.col("temperature") * F.col("temperature")
    - F.lit(0.016424828) * F.col("humidity") * F.col("humidity")
    + F.lit(0.002211732) * F.col("temperature") * F.col("temperature") * F.col("humidity")
    + F.lit(0.00072546) * F.col("temperature") * F.col("humidity") * F.col("humidity")
    - F.lit(0.000003582) * F.col("temperature") * F.col("temperature") * F.col("humidity") * F.col("humidity")
)

critical_temperatures_stream = (
    sensor_data
    .where((F.col("temperature") >= 32) | (F.col("temperature") <= -12))
    .withColumn(
        "severity",
        F.when(F.col("temperature") >= 40, F.lit("Excessive Heat"))
        .when(F.col("temperature") >= 32, F.lit("Heat Advisory"))
        .when(F.col("temperature") <= -18, F.lit("Extreme Cold Warning"))
        .otherwise(F.lit("Wind Chill Alert")),
    )
    .withColumn(
        "alert_reason",
        F.when(F.col("temperature") >= 32, F.lit("Heat advisory threshold (≥32°C)"))
        .otherwise(F.lit("Wind chill threshold (≤-12°C)")),
    )
    .withColumn(
        "heat_index_c",
        F.when(
            (F.col("temperature") >= 27) & (F.col("humidity") >= 40),
            heat_index_expr,
        ),
    )
    .select(
        "station_id",
        "temperature",
        "humidity",
        "timestamp",
        "latitude",
        "longitude",
        "severity",
        "alert_reason",
        "heat_index_c",
    )
)

average_readings_stream = (
    sensor_data
    .groupBy("station_id", F.window("timestamp", "1 minute"))
    .agg(
        F.avg("temperature").alias("avg_temperature"),
        F.avg("humidity").alias("avg_humidity"),
        F.first("latitude", ignorenulls=True).alias("latitude"),
        F.first("longitude", ignorenulls=True).alias("longitude"),
    )
    .select(
        "station_id",
        "latitude",
        "longitude",
        "avg_temperature",
        "avg_humidity",
        F.col("window").start.alias("window_start"),
        F.col("window").end.alias("window_end"),
    )
)

attention_needed_stream = (
    sensor_data
    .where((F.col("humidity") < 45) | (F.col("humidity") > 75))
    .groupBy("station_id")
    .agg(
        F.count("*").alias("critical_readings"),
        F.first("latitude", ignorenulls=True).alias("latitude"),
        F.first("longitude", ignorenulls=True).alias("longitude"),
    )
)

baseline_context_stream = (
    sensor_data
    .groupBy("station_id", F.window("timestamp", "7 days", "1 hour"))
    .agg(
        F.avg("temperature").alias("avg_temperature_7d"),
        F.avg("humidity").alias("avg_humidity_7d"),
    )
    .select(
        "station_id",
        "avg_temperature_7d",
        "avg_humidity_7d",
        F.col("window").start.alias("window_start"),
        F.col("window").end.alias("window_end"),
    )
)

# ---------- Helper writers ----------
def write_append(path: Path):
    def _fn(df, batch_id: int):
        df.coalesce(1).write.mode("append").parquet(str(path))
    return _fn

def write_overwrite(path: Path):
    def _fn(df, batch_id: int):
        df.coalesce(1).write.mode("overwrite").parquet(str(path))
    return _fn

# ---------- Streaming sinks ----------
critical_query = (
    critical_temperatures_stream.writeStream
    .outputMode("append")
    .foreachBatch(write_append(CRITICAL_PATH))
    .option("checkpointLocation", str(CHECKPOINT_DIR / "critical"))
    .queryName("CriticalTemperatures")
    .start()
)

average_query = (
    average_readings_stream.writeStream
    .outputMode("complete")
    .foreachBatch(write_overwrite(AVG_PATH))
    .option("checkpointLocation", str(CHECKPOINT_DIR / "avg"))
    .queryName("AverageReadings")
    .start()
)

attention_query = (
    attention_needed_stream.writeStream
    .outputMode("complete")
    .foreachBatch(write_overwrite(HUMIDITY_PATH))
    .option("checkpointLocation", str(CHECKPOINT_DIR / "humidity"))
    .queryName("AttentionNeeded")
    .start()
)

baseline_query = (
    baseline_context_stream.writeStream
    .outputMode("complete")
    .foreachBatch(write_overwrite(BASELINE_PATH))
    .option("checkpointLocation", str(CHECKPOINT_DIR / "baselines"))
    .queryName("StationBaseline")
    .start()
)

source_mode = "MOCK rate source" if USE_MOCK else f"NWS NDJSON from {INPUT_DIR}"
print(f"Streaming started ({source_mode}). Writing parquet snapshots under data/output/ ...")
print("Stop with Ctrl+C.")

critical_query.awaitTermination()
average_query.awaitTermination()
attention_query.awaitTermination()
baseline_query.awaitTermination()