// Databricks notebook source
// MAGIC %md
// MAGIC The dataset describe the US flight delays. It has five columns:
// MAGIC - The date column contains a string like 02190925. When converted, this maps to 02-19 09:25 am.
// MAGIC -  The delay column gives the delay in minutes between the scheduled and actual departure times. Early departures show negative numbers.
// MAGIC - The distance column gives the distance in miles from the origin airport to the destination airport.
// MAGIC - The origin column contains the origin IATA airport code.
// MAGIC -  The destination column contains the destination IATA airport code.
// MAGIC 
// MAGIC Using a schema, we’ll read the data into a DataFrame and reg
// MAGIC ister the DataFrame as a temporary view (more on temporary views shortly) so we
// MAGIC can query it with SQL
// MAGIC 
// MAGIC Normally, in a standalone Spark application, you will create a SparkSession instance
// MAGIC manually. However, in a Spark shell (or Data‐
// MAGIC bricks notebook), the SparkSession is created for you and accessible via the appro‐
// MAGIC priately named variable spark.
// MAGIC 
// MAGIC    **Read the dataset into a temporary view**

// COMMAND ----------

// Read our US departure flight data
val df = spark.read.format("csv")
 .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
 .option("header", "true")
 .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
 .load()

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Date is a string with year missing, so it might be difficult to do any queries using SQL year() function. So, we define a UDF to convert the date format into a legible formatt.

// COMMAND ----------

def toDateFormatUDF(dStr:String) : String  = {
  return s"${dStr(0)}${dStr(1)}${'/'}${dStr(2)}${dStr(3)}${' '}${dStr(4)}${dStr(5)}${':'}${dStr(6)}${dStr(7)}"
}

// test  it
toDateFormatUDF("02190925")

// COMMAND ----------

// Registrer the UDF
spark.udf.register("toDateFormatUDF", toDateFormatUDF(_:String):String)

// COMMAND ----------

// MAGIC %md
// MAGIC Now, we create a temporary view to which we can issye SQL queries

// COMMAND ----------

spark.udf.register("toDateFormatUDF", toDateFormatUDF(_:String):String)

val df1 = spark.sql("SELECT *, toDateFormatUDF(date) AS date_fm FROM us_delay_flights_tbl")

df1.createOrReplaceTempView("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Basic SQL Queries**
// MAGIC 
// MAGIC Now, let's try some SQL queries against this data set.
// MAGIC 
// MAGIC **1) Find out all flights whose distance between origin and destination is greater than 1000 miles**
// MAGIC 
// MAGIC As the table show, all of the longest flights were between Honolulu(HNL) and New Yorl (JFK).

// COMMAND ----------

spark.sql("""SELECT distance, origin, destination 
FROM us_delay_flights_tbl 
WHERE distance > 1000 
ORDER BY distance DESC""")
.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC we can do it with a dataframe equivalent query.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._ 

df.select("distance", "origin", "destination")
.where($"distance" > 1000)
.orderBy(desc("distance"))
.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **2) Find out all flights with at least 2 hour delays between San Francisco (SFO) and Chicago (ORD)**
// MAGIC 
// MAGIC It seem these delays is most common in winter months.

// COMMAND ----------

spark.sql("""SELECT date_fm, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN ='SFO' AND DESTINATION = 'ORD' 
ORDER BY delay DESC""")
.show(20, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **3) Label all US flights originating from airports with high, medium, low and no delays, regardless od destinations**

// COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
              CASE
                  WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay > 120 AND delay < 360 THEN  'Long Delays '
                  WHEN delay > 60 AND delay < 120 THEN  'Short Delays'
                  WHEN delay > 0 and delay < 60  THEN   'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'No Delays'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Creating SQL Databases and Tables**
// MAGIC 
// MAGIC Tables reside within a database. By default, Spark creates tables under the default database.
// MAGIC 
// MAGIC **1) Create a database called spark_db**

// COMMAND ----------

spark.sql("CREATE DATABASE spark_db")

// COMMAND ----------

// MAGIC %md
// MAGIC **2) Use that database**
// MAGIC 
// MAGIC Now, tables, that we create, will reside under the database spark_db

// COMMAND ----------

spark.sql("USE spark_db")

// COMMAND ----------

// MAGIC %md
// MAGIC **3) Create a table**
// MAGIC 
// MAGIC Spark allows you to create two types of tables:
// MAGIC - Managed table --> Spark manages both the metadata and the data in the file store. This could be a local filesystem, HDFS, or an object store such as Amazon S3 or Azure Blob. 
// MAGIC - Unmanaged table --> Spark only manages the metadata, while you manage the data yourself in an external data source such as Cassandra.
// MAGIC 
// MAGIC If we delete a unmanaged tables, it will delete only the metadata, not the actual data. While, with managed table, we will delete both.
// MAGIC 
// MAGIC ***3.1) Create a managed table***

// COMMAND ----------

// Using a SQL query
spark.sql("CREATE TABLE managed_us_delay_flights_tbl5 (date STRING, delay INT, distance INT, origin STRING, destination STRING)")


// COMMAND ----------

// Using the DataFrame API
val csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

val flights_df = spark.read.format("csv")
  .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
  .option("header", "true")
 .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
 .load()

flights_df.write.saveAsTable("managed_us_delay_flights_tbl13")

// COMMAND ----------

// MAGIC %md
// MAGIC ***3.2) Create a unmanaged table***
// MAGIC 
// MAGIC You can create unmanaged tables from your own data sources—say, Parquet, CSV, or JSON files stored in a file store accessible to your Spark application.

// COMMAND ----------

// Create an unmanaged table from a data source such as a CSV file in SQL
spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING) 
 USING csv
 OPTIONS (PATH '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')""")

// COMMAND ----------

// MAGIC %md
// MAGIC **4) Create view**
// MAGIC 
// MAGIC The difference between a view and a table is that views don’t actually hold the data; tables persist after your Spark application terminates, but views disappear.
// MAGIC 
// MAGIC You can create a view from an existing table using SQL. If you wish to work on only the subset of data set, you will create a view.

// COMMAND ----------

// MAGIC %md
// MAGIC **Global temporary and temporary views**

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Create a global temporary view with SQL (ONLY ORIGIN = 'SFO')
// MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination 
// MAGIC  from us_delay_flights_tbl 
// MAGIC  WHERE origin = 'SFO';

// COMMAND ----------

// MAGIC %md
// MAGIC When accessing a global temporary view you must use the prefix global_temp.<view_name>, because Spark creates global temporary views in a global temporary database called global_temp.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Show the previus global temp with origin = 'SFO'
// MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Create a temporary view (origin = 'JFK') with SQL
// MAGIC CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination 
// MAGIC  from us_delay_flights_tbl 
// MAGIC  WHERE origin = 'JFK';
// MAGIC  
// MAGIC  -- Show the temporary view
// MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Drop a view
// MAGIC DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;
// MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

// COMMAND ----------

// Create a global temporary view with the DataFrame API
val df1 =  spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view1")

// COMMAND ----------

// Create a temporary view with DataFrame API
df1.createOrReplaceTempView("us_origin_airport_SFO_tmp_view1")

// COMMAND ----------

// Drop a view
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

// COMMAND ----------

// MAGIC %md
// MAGIC **Temporary views versus global temporary views**
// MAGIC 
// MAGIC A temporary view is tied to a single SparkSession within a Spark application. In contrast, a global temporary view is visible across multiple SparkSessions within a Spark application. You can create multiple SparkSessions within a single Spark application—this can be handy, for example, in cases where you want to access (and combine) data from two different SparkSessions that don’t share the same Hive metastore configurations.

// COMMAND ----------

// MAGIC %md
// MAGIC **Read tables into DataFrames** 
// MAGIC 
// MAGIC Using SQL to query the table and assign the returned result to a DataFrame:

// COMMAND ----------

val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
val usFlightsDF2 = spark.table("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC Now you have a cleansed DataFrame read from an existing Spark SQL table.
