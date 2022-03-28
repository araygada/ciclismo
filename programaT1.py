from msilib import schema
from operator import truediv
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, udf
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StringType, StructType, TimestampType)

spark = SparkSession.builder.appName("Ciclismo").getOrCreate()


print("\n")
print("=" * 87)
print("Este programa se basa en información sobre 44 ciclistas y permite establecer ")
print("el top n de los resultados de los deportistas tanto en kilómetros totales ")
print("como en promedio de kilómetros diarios \n")


mensaje = "Indique un número entre 1 y 44 para determinar la clasificación top correspondiente:"

def nTop(message):

    ans = 0
    while not ans:
        try:
            ans = int(input(message))
            if ans < 1 or ans > 44:
                raise ValueError
        except ValueError:
            ans = 0
            print("That's not an option!")
    return ans


#myTop = nTop(mensaje)

#print(f"El númeor de top es: {myTop}")

# Carga del csv de ciclistas
# ---------------------------------------------------------------------------------------------------
csv_schema = StructType([StructField('id', IntegerType()),          # Cédula (numérico)
                        StructField('nombre', StringType()),        # Nombre completo (string)
                        StructField('provincia', StringType())])    # Provincia (string)

df_ciclistas = spark.read.csv('ciclista.csv', schema=csv_schema, header=True)
df_ciclistas.show()

# Carga del csv de rutas
# ---------------------------------------------------------------------------------------------------
csv_schema = StructType([StructField('id', IntegerType()),          # Código de ruta (id numérico) 
                        StructField('nombre', StringType()),        # Nombre ruta (string)
                        StructField('kilómetros', FloatType())])    # Kilómetros (numérico / decimal)

df_rutas = spark.read.csv('ruta.csv', schema=csv_schema, header=True)
df_rutas.show()

# Carga del csv de actividades
# ---------------------------------------------------------------------------------------------------
csv_schema = StructType([StructField('id_ruta', IntegerType()),     # Código de ruta (numérico)
                        StructField('id_ciclista', StringType()),   # Cédula (numérico)
                        StructField('fecha', StringType())])          # Fecha (Formato YYYY-MM-DD)

df_actividades = spark.read.csv('actividad.csv', schema=csv_schema, header=True)
df_actividades.show()

formatted_df = df_actividades.withColumn('fecha2',date_format(col('fecha'), 'MM/DD/YYYY'))
formatted_df.show()
