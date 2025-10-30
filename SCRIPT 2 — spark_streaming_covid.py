# spark_streaming_covid.py
# Procesamiento en tiempo real: Casos COVID-19 en Colombia
# Autor: Emerson Manuel Basilio Navarro
# Curso: Big Data y Análisis de Datos Masivos - UNAD
# Año: 2025

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StringType

# --------------------------------------------------------
# 1. Crear sesión Spark Streaming
# --------------------------------------------------------
spark = SparkSession.builder \
    .appName("COVID19_Colombia_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------------
# 2. Definir esquema de los datos que llegan por Kafka
# --------------------------------------------------------
schema = StructType() \
    .add("id_de_caso", StringType()) \
    .add("fecha_reporte_web", StringType()) \
    .add("departamento_nom", StringType()) \
    .add("ciudad_municipio_nom", StringType()) \
    .add("sexo", StringType()) \
    .add("fuente_tipo_contagio", StringType()) \
    .add("estado", StringType())

# --------------------------------------------------------
# 3. Lectura desde Kafka
# --------------------------------------------------------
# En la VM, asegúrate de que Zookeeper y Kafka estén activos
# y que hayas creado un topic llamado "covid_topic"

kafka_bootstrap = "localhost:9092"
topic = "covid_topic"

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .load()

# --------------------------------------------------------
# 4. Transformación de datos del topic
# --------------------------------------------------------
df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Contar casos por departamento en tiempo real
casos_por_departamento = df_parsed.groupBy("departamento_nom").count()

# --------------------------------------------------------
# 5. Visualización en consola (en tiempo real)
# --------------------------------------------------------
query = casos_por_departamento.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("📡 Procesando datos en tiempo real desde Kafka...")
query.awaitTermination()
