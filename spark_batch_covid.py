# spark_batch_covid.py
# Procesamiento batch: Casos COVID-19 en Colombia
# Autor: Emerson Manuel Basilio Navarro
# Curso: Big Data y Análisis de Datos Masivos - UNAD
# Año: 2025

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, to_date, year, month, dayofmonth

# --------------------------------------------------------
# 1. Crear la sesión de Spark
# --------------------------------------------------------
spark = SparkSession.builder.appName("COVID19_Colombia_Batch").getOrCreate()

# --------------------------------------------------------
# 2. Cargar el conjunto de datos (desde Datos Abiertos)
# --------------------------------------------------------
data_path = "https://www.datos.gov.co/resource/gt2j-8ykr.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)
print("✅ Total de registros cargados:", df.count())

# --------------------------------------------------------
# 3. Limpieza y transformación
# --------------------------------------------------------
df = df.withColumnRenamed("fecha_reporte_web", "fecha") \
       .withColumnRenamed("departamento_nom", "departamento") \
       .withColumnRenamed("ciudad_municipio_nom", "municipio")

df_clean = df.dropna(subset=["fecha", "departamento", "municipio"])

df_clean = df_clean.withColumn("fecha", to_date(col("fecha"))) \
                   .withColumn("anio", year(col("fecha"))) \
                   .withColumn("mes", month(col("fecha"))) \
                   .withColumn("dia", dayofmonth(col("fecha")))

print("✅ Registros limpios:", df_clean.count())

# --------------------------------------------------------
# 4. Análisis exploratorio (EDA)
# --------------------------------------------------------
print("\n📊 Casos por departamento:")
casos_departamento = df_clean.groupBy("departamento") \
    .agg(count("*").alias("total_casos")) \
    .orderBy(desc("total_casos"))
casos_departamento.show(10, truncate=False)

if "sexo" in df_clean.columns:
    print("\n📊 Casos por sexo:")
    df_clean.groupBy("sexo").agg(count("*").alias("total")).orderBy(desc("total")).show()

if "estado" in df_clean.columns:
    print("\n📊 Casos por estado:")
    df_clean.groupBy("estado").agg(count("*").alias("total")).orderBy(desc("total")).show()

# --------------------------------------------------------
# 5. Almacenamiento de resultados
# --------------------------------------------------------
output_path = "salida_covid_batch"
casos_departamento.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path + "/casos_departamento")

print(f"\n💾 Resultados guardados en: {output_path}/")
spark.stop()

