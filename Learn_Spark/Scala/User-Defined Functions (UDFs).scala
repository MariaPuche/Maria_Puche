// Databricks notebook source
This notebook contain a simplified example of creating a Spark SQL UDF.

// COMMAND ----------

// Create cubed function
val cubed = (s:Long) => {
  s*s*s
}

// Register UDF
spark.udf.register("cubed",cubed)

// Create temporary view
spark.range(1,9).createOrReplaceTempView("udf_test")

// COMMAND ----------

Now, we use Spark SQL to execute these cubed() functions.

// COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
