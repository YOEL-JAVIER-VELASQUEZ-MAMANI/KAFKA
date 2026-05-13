from pyspark.sql import SparkSession

# Crear sesión Spark
spark = SparkSession.builder.appName("RDDExample").getOrCreate()

# Leer archivo CSV como RDD
rdd = spark.sparkContext.textFile("data/raw/ventas.csv")

# Contar registros
print("Cantidad de registros:")
print(rdd.count())

# Mostrar primeras líneas
print("Primeras líneas:")
for linea in rdd.take(5):
    print(linea)

spark.stop()