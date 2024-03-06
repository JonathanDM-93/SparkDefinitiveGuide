# Ejemplo donde se carga un archivo csv

# Cargar librerias

from pyspark import SparkFiles
from pyspark.sql import SparkSession


# Crear sesión
spark = SparkSession.builder.appName("EndtoEnd").getOrCreate()

# El tipo de archivo PySpark puede leer otros formatos como: json, parquet, orc
# Cargar el archivo que viene la ruta especificada en el libro

# URL del archivo CSV en GitHub (enlace raw)
url_csv = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/csv/2015-summary.csv"

# Descargar el archivo CSV localmente
spark.sparkContext.addFile(url_csv)
local_path = "file://" + SparkFiles.get("2015-summary.csv")

# Cargar el conjunto de datos desde el archivo CSV local
flight2015 = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(local_path)

flight2015.show(10, False)

# Register the DataFrame as a SQL temporary view
flight2015.createOrReplaceTempView("View")
