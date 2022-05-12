// Databricks notebook source
// MAGIC %md
// MAGIC First, let's create a sample data set.

// COMMAND ----------

// Create a dataframe
val t1 = Array(35, 36, 32, 30, 40, 42, 38)
val t2 = Array(31, 32, 34, 55, 56)
val tC = Seq(t1, t2).toDF("celsisus")
tC.createOrReplaceTempView("tC")

// COMMAND ----------

tC.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## FUNCIONES PARA TRABAJAR CON ARRAY

// COMMAND ----------

// MAGIC %md
// MAGIC #### TRANFORM()
// MAGIC Produces an array by applying a function to each element of the input array (similar to a map() function):

// COMMAND ----------

// Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""
SELECT celsisus, transform(celsisus, t -> ((t*9) div 5) +31) as Fahrenheit FROM tC""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### FILTER()
// MAGIC Produces an array consisting of only the elements of the input array for which the Boolean function is true: 

// COMMAND ----------

// Filter temperatures > 38  for array of temperatures
spark.sql(""" 
SELECT celsisus, filter(celsisus, t -> t > 38) as High
FROM tC"""). show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### EXISTS()
// MAGIC Returns true if the boolean functions holds for any element in the input array:
// MAGIC (En el resutlado, muestra que en el primer array si hay un 38 y en el segundo no)

// COMMAND ----------

// Is there a temperature of 38C in the array of temperatures
spark.sql(""" 
SELECT celsisus, exists(celsisus, t -> t = 38) as threshold
FROM tC""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### REDUCE()
// MAGIC Reduces de elements of the array to a single value by merging the elements into a buffer B using functions <B, T, B > and applying a finishing function <B, R> on the final buffer:

// COMMAND ----------

// Calculate sum temperature 
spark.sql("""
SELECT celsisus,
       reduce(celsisus,
              0,
              (t, acc) -> t + acc) as Sum
FROM tC""").show()

// COMMAND ----------

// Calculate average temperature
spark.sql("""
SELECT celsisus, 
        reduce(celsisus,
               0, 
               (t, acc) -> t + acc,
               acc -> acc div size(celsisus))
               as avgCelsius
FROM tC""").show()

// COMMAND ----------

// Calculate average temperature and convert to Fanrenheit
spark.sql("""
SELECT celsisus, 
        reduce(celsisus,
               0, 
               (t, acc) -> t + acc,
               acc -> (acc div size(celsisus) * 9 div 5) +32)
               as avgFhanrenheit
FROM tC""").show()
