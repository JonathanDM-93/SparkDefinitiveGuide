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

flight2015.show(10, False)
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