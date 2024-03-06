# Ejemplo donde se carga un archivo csv

# Cargar librerias
from pyspark.sql import SparkSession

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
flight2015.createOrReplaceTempView("View")

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
