"""
Archivo: 04_streaming_spark_kafka.py
Proyecto: Sistema Comercial Big Data

Objetivo:
Consumir eventos desde Kafka usando Spark Structured Streaming.

Entrada:
- Topic Kafka: ventas-stream

Salida:
- Consola
- output/streaming/events/
- output/streaming/resumen_categorias.csv
- output/streaming/resumen_ciudades.csv
- output/streaming/alertas_fraude.csv

Comando:
docker compose exec spark python src/04_streaming_spark_kafka.py --duration 120
"""

from pathlib import Path
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# ============================================================
# RUTAS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

OUTPUT_DIR = BASE_DIR / "output" / "streaming"
EVENTS_DIR = OUTPUT_DIR / "events"

CHECKPOINT_DIR = BASE_DIR / "data" / "checkpoints"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# KAFKA
# ============================================================

KAFKA_TOPIC = "ventas-stream"
KAFKA_BOOTSTRAP_SERVERS = "rapidex-broker:9092"

KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview2"
)

# ============================================================
# ESQUEMA JSON
# ============================================================

schema = StructType([
    StructField("evento_id", StringType(), True),
    StructField("venta_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("riesgo_fraude", DoubleType(), True),
    StructField("timestamp_evento", StringType(), True)
])

# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("VentasStreamingKafka")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

# ============================================================
# PROCESAMIENTO BATCH
# ============================================================

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: sin datos")
        return

    print("=" * 80)
    print(f"BATCH {batch_id}")
    print("=" * 80)

    batch_df.cache()

    print("Eventos recibidos:")
    batch_df.show(10, truncate=False)

    # ========================================================
    # RESUMEN CATEGORÍAS
    # ========================================================

    resumen_categoria_df = (
        batch_df
        .groupBy("categoria")
        .agg(
            F.count("*").alias("total_ventas"),
            F.round(F.sum("total"), 2).alias("monto_total"),
            F.round(F.avg("total"), 2).alias("ticket_promedio")
        )
        .orderBy(F.desc("monto_total"))
    )

    print("\nResumen categorías:")
    resumen_categoria_df.show(truncate=False)

    # ========================================================
    # RESUMEN CIUDADES
    # ========================================================

    resumen_ciudad_df = (
        batch_df
        .groupBy("ciudad")
        .agg(
            F.count("*").alias("total_ventas"),
            F.round(F.sum("total"), 2).alias("monto_total")
        )
        .orderBy(F.desc("monto_total"))
    )

    print("\nResumen ciudades:")
    resumen_ciudad_df.show(truncate=False)

    # ========================================================
    # ALERTAS FRAUDE
    # ========================================================

    alertas_df = (
        batch_df
        .filter(F.col("riesgo_fraude") >= 0.7)
        .select(
            "evento_id",
            "venta_id",
            "categoria",
            "metodo_pago",
            "ciudad",
            "total",
            "riesgo_fraude"
        )
        .orderBy(F.desc("riesgo_fraude"))
    )

    print("\nAlertas fraude:")
    alertas_df.show(truncate=False)

    # ========================================================
    # GUARDAR CSV
    # ========================================================

    eventos_pdf = batch_df.toPandas()

    eventos_pdf.to_csv(
        EVENTS_DIR / f"batch_{batch_id}.csv",
        index=False
    )

    resumen_categoria_pdf = resumen_categoria_df.toPandas()

    resumen_categoria_pdf.to_csv(
        OUTPUT_DIR / "resumen_categorias.csv",
        mode="a",
        header=not (OUTPUT_DIR / "resumen_categorias.csv").exists(),
        index=False
    )

    resumen_ciudad_pdf = resumen_ciudad_df.toPandas()

    resumen_ciudad_pdf.to_csv(
        OUTPUT_DIR / "resumen_ciudades.csv",
        mode="a",
        header=not (OUTPUT_DIR / "resumen_ciudades.csv").exists(),
        index=False
    )

    alertas_pdf = alertas_df.toPandas()

    if len(alertas_pdf) > 0:

        alertas_pdf.to_csv(
            OUTPUT_DIR / "alertas_fraude.csv",
            mode="a",
            header=not (OUTPUT_DIR / "alertas_fraude.csv").exists(),
            index=False
        )

    batch_df.unpersist()

    print(f"Batch {batch_id} procesado correctamente")

# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--duration",
        type=int,
        default=120
    )

    args = parser.parse_args()

    spark = create_spark_session()

    print("=" * 80)
    print("INICIANDO STREAMING")
    print("=" * 80)

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option("subscribe", "ventas-online")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING)")
        .withColumn(
            "json_data",
            F.from_json(F.col("value"), schema)
        )
        .select("json_data.*")
        .withColumn(
            "timestamp_evento",
            F.to_timestamp("timestamp_evento")
        )
    )

    query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        str(CHECKPOINT_DIR)
    )
    .start()
)

    query.awaitTermination(args.duration)

    query.stop()

    print("=" * 80)
    print("STREAMING FINALIZADO")
    print("=" * 80)

    spark.stop()

# ============================================================
# EJECUCIÓN
# ============================================================

if __name__ == "__main__":
    main()