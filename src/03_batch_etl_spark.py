"""
Archivo: 03_batch_etl_spark.py
Proyecto: Sistema Comercial Big Data
"""

from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# RUTAS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"

PROCESSED_DIR = BASE_DIR / "data" / "processed"

KPI_DIR = BASE_DIR / "output" / "kpis"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

KPI_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("SistemaComercialETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# LECTURA
# ============================================================

def read_csv(spark, filename):

    path = str(RAW_DIR / filename)

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def read_json(spark, filename):

    path = str(RAW_DIR / filename)

    return (
        spark.read
        .option("multiline", "true")
        .json(path)
    )


# ============================================================
# GUARDAR CSV
# ============================================================

def write_single_csv(df, output_filename):

    final_path = KPI_DIR / output_filename

    temp_dir = KPI_DIR / f"tmp_{output_filename}"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(temp_dir))
    )

    part_file = list(temp_dir.glob("part-*.csv"))[0]

    shutil.move(str(part_file), str(final_path))

    shutil.rmtree(temp_dir)

    print(f"Reporte creado: {output_filename}")


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 80)
    print("INICIANDO ETL")
    print("=" * 80)

    spark = create_spark_session()

    # ========================================================
    # EXTRACT
    # ========================================================

    productos_df = read_csv(spark, "productos.csv")

    ventas_df = read_csv(spark, "ventas.csv")

    clientes_df = read_json(spark, "clientes.json")

    ventas_online_df = read_json(spark, "ventas.json")

    # ========================================================
    # TRANSFORM
    # ========================================================

    ventas_clean_df = (
        ventas_df
        .withColumn(
            "fecha_venta",
            F.to_timestamp("fecha_venta")
        )
        .withColumn(
            "cantidad",
            F.col("cantidad").cast("int")
        )
        .withColumn(
            "precio_unitario",
            F.col("precio_unitario").cast("double")
        )
        .withColumn(
            "subtotal",
            F.col("subtotal").cast("double")
        )
        .withColumn(
            "descuento",
            F.col("descuento").cast("double")
        )
        .withColumn(
            "total",
            F.col("total").cast("double")
        )
    )

    # ========================================================
    # LIMPIEZA COLUMNAS DUPLICADAS
    # ========================================================

    productos_clean_df = (
        productos_df
        .withColumnRenamed(
            "categoria",
            "categoria_producto"
        )
        .withColumnRenamed(
            "fecha_registro",
            "fecha_registro_producto"
        )
    )

    clientes_clean_df = (
        clientes_df
        .withColumnRenamed(
            "fecha_registro",
            "fecha_registro_cliente"
        )
    )

    # ========================================================
    # JOINS
    # ========================================================

    ventas_enriched_df = (
        ventas_clean_df
        .join(
            productos_clean_df,
            on="producto_id",
            how="left"
        )
        .join(
            clientes_clean_df,
            on="cliente_id",
            how="left"
        )
    )

    print("\nVista previa:")

    ventas_enriched_df.select(
        "venta_id",
        "nombre_producto",
        "categoria_producto",
        "cantidad",
        "total",
        "metodo_pago",
        "ciudad"
    ).show(10, truncate=False)

    # ========================================================
    # LOAD PARQUET
    # ========================================================

    print("\nGuardando parquet...")

    parquet_path = PROCESSED_DIR / "ventas_clean.parquet"

    if parquet_path.exists():
        shutil.rmtree(parquet_path)

    (
        ventas_enriched_df
        .write
        .mode("overwrite")
        .parquet(str(parquet_path))
    )

    print("Parquet generado correctamente")

    # ========================================================
    # SPARK SQL
    # ========================================================

    ventas_enriched_df.createOrReplaceTempView("ventas")

    # ========================================================
    # KPI 1
    # ========================================================

    ventas_categoria_df = spark.sql("""
        SELECT
            categoria_producto,
            COUNT(*) AS total_ventas,
            ROUND(SUM(total), 2) AS monto_total
        FROM ventas
        GROUP BY categoria_producto
        ORDER BY monto_total DESC
    """)

    ventas_categoria_df.show()

    write_single_csv(
        ventas_categoria_df,
        "ventas_por_categoria.csv"
    )

    # ========================================================
    # KPI 2
    # ========================================================

    productos_top_df = spark.sql("""
        SELECT
            nombre_producto,
            categoria_producto,
            SUM(cantidad) AS cantidad_vendida,
            ROUND(SUM(total), 2) AS ingresos
        FROM ventas
        GROUP BY nombre_producto, categoria_producto
        ORDER BY cantidad_vendida DESC
        LIMIT 20
    """)

    productos_top_df.show()

    write_single_csv(
        productos_top_df,
        "productos_top.csv"
    )

    # ========================================================
    # KPI 3
    # ========================================================

    metodos_pago_df = spark.sql("""
        SELECT
            metodo_pago,
            COUNT(*) AS operaciones,
            ROUND(SUM(total), 2) AS monto_total
        FROM ventas
        GROUP BY metodo_pago
        ORDER BY monto_total DESC
    """)

    metodos_pago_df.show()

    write_single_csv(
        metodos_pago_df,
        "metodos_pago.csv"
    )

    # ========================================================
    # KPI 4
    # ========================================================

    ventas_ciudad_df = spark.sql("""
        SELECT
            ciudad,
            COUNT(*) AS total_ventas,
            ROUND(SUM(total), 2) AS ingresos
        FROM ventas
        GROUP BY ciudad
        ORDER BY ingresos DESC
    """)

    ventas_ciudad_df.show()

    write_single_csv(
        ventas_ciudad_df,
        "ventas_por_ciudad.csv"
    )

    # ========================================================
    # KPI 5
    # ========================================================

    clientes_premium_df = spark.sql("""
        SELECT
            cliente_id,
            nombre,
            ciudad,
            COUNT(*) AS compras,
            ROUND(SUM(total), 2) AS gasto_total
        FROM ventas
        WHERE segmento = 'premium'
        GROUP BY cliente_id, nombre, ciudad
        ORDER BY gasto_total DESC
        LIMIT 20
    """)

    clientes_premium_df.show()

    write_single_csv(
        clientes_premium_df,
        "clientes_premium.csv"
    )

    # ========================================================
    # RDD
    # ========================================================

    ventas_rdd = spark.sparkContext.textFile(
        str(RAW_DIR / "ventas.csv")
    )

    header = ventas_rdd.first()

    metodo_pago_rdd = (
        ventas_rdd
        .filter(lambda x: x != header)
        .map(lambda x: x.split(",")[8])
        .map(lambda x: (x, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    rdd_df = spark.createDataFrame(
        metodo_pago_rdd,
        ["metodo_pago", "total"]
    )

    rdd_df.show()

    write_single_csv(
        rdd_df,
        "rdd_metodos_pago.csv"
    )

    print("=" * 80)
    print("ETL FINALIZADO")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()