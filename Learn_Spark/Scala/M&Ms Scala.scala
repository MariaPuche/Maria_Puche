// Databricks notebook source
// MAGIC %md
// MAGIC ## **Analizar el color favorito de los M&Ms de los estudiantes en los diferentes estados**
// MAGIC 
// MAGIC Primero, descargar los datos de internet. Luego, cargar el fichero de datos  descargado en databricks:
// MAGIC   - Ir al panel izquierdo y pinchar en data.
// MAGIC   - Create table
// MAGIC   - Subir archivo arrastrando.
// MAGIC   - Pinchar en create table in Notebook para obtener la ruta del archivo
// MAGIC   - Pinchar en create table with UI para configurar nuestros datos.
// MAGIC   - Por último, volver a pinchar en create table.

// COMMAND ----------

// MAGIC %md
// MAGIC **1) Importar librerias**

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC **2) Leer el csv con esquema definido**

// COMMAND ----------

val mnmDF = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("/FileStore/tables/mnm_dataset.csv")

// COMMAND ----------

// Mostrar los datos
display(mnmDF.limit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC **3) Recuento total de los colores de los M&Ms por estado y color, por orden descendiente**  

// COMMAND ----------

val countMnMDF=mnmDF
    .select("State","Color","Count")
    .groupBy("State","Color")
    .agg(count("Count").alias("Total"))
    .orderBy(desc("Total"))

countMnMDF.show(60)
println(s"Total Rows = ${countMnMDF.count()}")


// COMMAND ----------

// MAGIC %md
// MAGIC **3) Recuento total de los colores de los M&Ms en California (CA), por orden descendiente**  

// COMMAND ----------

val caCountMnMDF = mnmDF
 .select("State", "Color", "Count")
 .where($"State" === "CA")
 .groupBy("State", "Color")
 .agg(count("Count").alias("Total"))
 .orderBy(desc("Total"))

 caCountMnMDF.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC **3.1) Recuento total de los colores de los M&Ms en CA, NV y CO, por orden descendiente**  

// COMMAND ----------

val CCountMnMDF = mnmDF
 .select("State", "Color", "Count")
 .where($"State" === "CA" || $"State" === "NV" || $"State" === "CO")
 .groupBy("State", "Color")
 .agg(count("Count").alias("Total"))
 .orderBy(desc("Total"))

 CCountMnMDF.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC **4) Calcular la media total de count**

// COMMAND ----------

mnmDF.agg(avg("Count")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC **5) Calcular la media de count por estado**

// COMMAND ----------

val avg_count = mnmDF
      .select("Count","State")
      .groupBy("State")
      .count()
      .orderBy(desc("count"))

avg_count.show()

// COMMAND ----------

// MAGIC %md
// MAGIC **6) Calcular el maximo, minimo y total de count**

// COMMAND ----------

mnmDF.select(sum("Count").alias("Total"),max("Count").alias("Máximo"),min("Count").alias("Minimo"),avg("Count").alias("Media"),count("Count")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC **7) Crear una tmpView para realizar una consulta SQL**

// COMMAND ----------

mnmDF.createOrReplaceTempView("MnMSQL")

// COMMAND ----------

val Mnm1 = spark.sql("Select * from MnMSQL").show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC **8) Obtener la estructura del schema dado por defecto al cargar el csv**

// COMMAND ----------

println(mnmDF.printSchema)

// COMMAND ----------

println(mnmDF.schema)
