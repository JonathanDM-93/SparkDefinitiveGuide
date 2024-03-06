# Ejemplo donde se carga un archivo csv

# Cargar librerias
from pyspark.sql import SparkSession
from pyspark.sql.functions import max

# Crear sesión
spark = SparkSession.builder.appName("EndtoEnd").getOrCreate()

# Ruta del archivo
local_path = "C:/Users/joni_/Downloads/2015-summary.csv"

# Cargar el conjunto de datos desde el archivo CSV local
flight2015 = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(local_path)

# Register the DataFrame as a SQL temporary view

# flight2015.createOrReplaceTempView("tableA")
flight2015.createGlobalTempView("tableA")

# flight2015.show(10, False)
# +-----------------+-------------------+-----+
# |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
# +-----------------+-------------------+-----+
# |United States    |Romania            |15   |
# |United States    |Croatia            |1    |
# |United States    |Ireland            |344  |
# |Egypt            |United States      |15   |
# |United States    |India              |62   |
# |United States    |Singapore          |1    |
# |United States    |Grenada            |62   |
# |Costa Rica       |United States      |588  |
# |Senegal          |United States      |40   |
# |Moldova          |United States      |1    |
# +-----------------+-------------------+-----+
# only showing top 10 rows

# ** La lectura de una tabla es una trasformación.

# -------------------------------------------------------------------------------------------------------------------- #
# Pag. 23 - 24
# Ver el plan físico de Spark
# flight2015.sort("count").explain()

# Podemos dar la patada inicial de este plan; sin embargo, antes de hacer eso vamos a marcar la configuración. Por
# defecto cuando realizamos un shuffle, Spark devuelve 200 particiones. Configuremos este valor a 5 para reducir el
# número de particiones de sálida del shuffle:
spark.conf.set("spark.sql.shuffle.partitions", 5)
# print(flight2015.sort("count").take(5))

# [Row(DEST_COUNTRY_NAME='Malta', ORIGIN_COUNTRY_NAME='United States', count=1),
# Row(DEST_COUNTRY_NAME='Saint Vincent and the Grenadines', ORIGIN_COUNTRY_NAME='United States', count=1),
# Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Croatia', count=1), Row(DEST_COUNTRY_NAME='United States',
# ORIGIN_COUNTRY_NAME='Gibraltar', count=1), Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Singapore',
# count=1)]

# El plan lógico de transformaciones de spark que se construyo arriba define el linaje para el DF dado un punto en el
# tiempo, spark conoce como re-computar cada partición ejecutando todas las operaciones como si lo hubiera hecho antes
# en la misma data de entrada.
# Esto sienta el corazon del modelo de programación de spark - programación funcional cuando las mismas entradas resul-
# tan las mismas sálidas cuando las transformaciones en la data permanecen constantes.

# Nosotros no manipularemos la data física, en cambio, configuraremos la ejecución física a traves de cosas como el
# parametro del número[shuffle] particiones. Cambiar este valor puede ayudar a controlar las caracteristicas
# del plan de ejecución de tus jobs de spark.

# spark way
flight2015_spark = flight2015.groupBy("DEST_COUNTRY_NAME").count()
# flight2015_spark.show()

# Plan físico de ejecución
# flight2015_spark.explain()
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- HashAggregate(keys=[DEST_COUNTRY_NAME#17], functions=[count(1)])
#    +- Exchange hashpartitioning(DEST_COUNTRY_NAME#17, 5), ENSURE_REQUIREMENTS, [plan_id=33]
#       +- HashAggregate(keys=[DEST_COUNTRY_NAME#17], functions=[partial_count(1)])
#          +- FileScan csv [DEST_COUNTRY_NAME#17] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/C:/Users/joni_/Downloads/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

# Vamos a usar algunas funciones de spark y para ello es necesario importarlas
# Aqui creo un nuevo nombre de DF, llamo al DF y con el operador punto llamo a select y el campo "count" y llamo
# a la función y nuevamente con el operador punto llamo a la funcion alias para renombrar el campo.

flight2015_max = flight2015.select(max("count").alias("Valor_maximo"))
# flight2015_max.show()
# +------------+
# |Valor_maximo|
# +------------+
# |      370002|
# +------------+


