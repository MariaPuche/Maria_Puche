// Databricks notebook source
// MAGIC %md
// MAGIC ## Load a DataFrame
// MAGIC First, we **load a Dataframe froam a data source**. So, we use a interface of Sparw, **DataFrameReader**. It's enable to read data into a DataFrame from myriad data sources in formats such as JSON, CSV, Parquet, Text...
// MAGIC 
// MAGIC In particular, we want to have a distributed DataFrame composed of San Francisco Fire Department calls in memory. Then, we'll examine specific aspects of our SF Fire Departament.
// MAGIC 
// MAGIC First, we won't specify the schema and we'll use the samplingRatio option:

// COMMAND ----------

val sampleDF = spark
  .read
  .option("samplingRatio", 0.001)
  .option("header", true)
  .csv("dbfs:/FileStore/shared_uploads/maria.puche@bosonit.com/sf_fire_calls.csv")


// COMMAND ----------

// MAGIC %md
// MAGIC Now, we load a dataframe specify the schema. This dataframe will be used throughout the exercise.

// COMMAND ----------

// 1º) Define our schema using DDL
val fireSchema = "CallNumber INT, UnitID STRING, IncidentNumber Int, CallType STRING, CallDate STRING, WatchDate STRING, CallFinalDisposition STRING, AvailableDtTm STRING, Address STRING, City STRING, ZipCode INT, Battalion STRING, StationArea STRING, Box STRING, OriginalPriority STRING, Priority STRING, FinalPriority INT, ALSUnit BOOLEAN, CallTypeGroup STRING, NumAlarms INT, UnitType STRING, UnitSequenceInCallDispatch INT, FirePreventionDistrict STRING, SupervisorDistrict STRING, Neighborhood STRING, Location STRING, RowID STRING, Delay FLOAT "

// 2º) Read the file using CSV DataFrameReader
// Define the location of the public dataset on the S3 bucket
val sfFireFile = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

// Load a file
val fireDF = spark.read.schema (fireSchema).option("header","true").csv(sfFireFile)


// COMMAND ----------

// Inspect the data
display(fireDF.limit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Write a DataFrame into a external data soucer
// MAGIC 
// MAGIC To write the DataFrame into an external data source in your format of choice, you can use the **DataFrameWriter** interface. Like DataFrameReader, it supports multiple
// MAGIC data sources.

// COMMAND ----------

// To save a DataFrame as a Parquet File
val parquetPath = "dbfs:/FileStore/shared_uploads/maria.puche@bosonit.com/EJEMPLO2"
fireDF.write.format("parquet").save(parquetPath)

// COMMAND ----------

// To save a DataFrame as a SQL table
val parquetTable = "tabla2"
fireDF.write.format("parquet").saveAsTable(parquetTable)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Common operations to perform on DataFrames
// MAGIC 
// MAGIC In Spark, projections are done with the **select()** method, while filters can be expressed using the **filter()** or **where()** method. We can use this technique to examine specific aspects of our SF Fire Department data set.

// COMMAND ----------

// MAGIC %md
// MAGIC **1) Compute the numbers of row contained in our dataset**
// MAGIC 
// MAGIC There are 430660 rows in our dataframe.

// COMMAND ----------

fireDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC **2) Filter out "Medical Incident" call types and select "IncidentNumber", "AvailableDtTm" and "CallType"**

// COMMAND ----------

val fewFireDF = fireDF
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where($"CallType" != "Medical Incident") 

fewFireDF.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **2) How many distinct types of calls were made to the Fire Department?**
// MAGIC 
// MAGIC There are 32 diferent types of calls.

// COMMAND ----------

import org.apache.spark.sql.functions._

fireDF
  .select("CallType")
  .where($"CallType".isNotNull)
  .distinct()
  .count()

// COMMAND ----------

// MAGIC %md
// MAGIC **3) What are distinct types of calls were made to the Fire Department?**
// MAGIC 
// MAGIC These are all the distinct type of call to the San Francisco Fire Department.

// COMMAND ----------

fireDF
  .select("CallType")
  .where($"CallType".isNotNull)
  .distinct()
  .show(32,truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC **4) Find out all response or delayed times greater than 5 mins**

// COMMAND ----------

// Rename the column Delay and retunr a new DataFrame(new_fire_df)
val new_fireDF = fireDF.withColumnRenamed("Delay","ResponseDeLayedinMins")

// Find out all calls where the response time to the fire site was delayed for more tan 5 mins.
new_fireDF
    .select("ResponseDeLayedinMins")
    .where($"ResponseDeLayedinMins" > 5)
    .show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **5) Transform the string dates to spark Timestamp dat type**

// COMMAND ----------

val fire_tsDF = new_fireDF
            .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
            .drop("CallDate")
            .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
            .drop("WatchDate")
            .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
            .drop("AvailableDtTm")


// COMMAND ----------

// MAGIC %md
// MAGIC Now, we check the transformed columns with Spark Timestamp type

// COMMAND ----------

(fire_tsDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, false))

// COMMAND ----------

// MAGIC %md
// MAGIC **6) How many distinct years of data is in the CSV file?**
// MAGIC 
// MAGIC In all, we have fire calls from years 2000 - 2018

// COMMAND ----------

fire_tsDF
 .select(year($"IncidentDate"))
 .distinct()
 .orderBy(year($"IncidentDate"))
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC **7) What were the most common types of fire calls??**
// MAGIC 
// MAGIC It appears that Medical Incidents is the most common type of fire calls.

// COMMAND ----------

fire_tsDF
 .select("CallType")
 .where($"CallType".isNotNull)
 .groupBy("CallType")
 .count()
 .orderBy(desc("count"))
 .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **8) What zip codes accounted for most common calls?**
// MAGIC 
// MAGIC The most common calls were all related to Medical Incident, and the two zip codes are 94102 and 94103

// COMMAND ----------

(fire_tsDF
 .select("CallType", "ZipCode")
 .where($"CallType".isNotNull)
 .groupBy("CallType", "Zipcode")
 .count()
 .orderBy(desc("count"))
 .show(10, false))

// COMMAND ----------

// MAGIC %md
// MAGIC **9) What San Francisco neighborhoods are in the zip codes 94102 and 94103?**
// MAGIC 
// MAGIC Following list them.

// COMMAND ----------

fire_tsDF
  .select("Neighborhood", "Zipcode")
  .where((col("Zipcode") === 94102) || (col("Zipcode") === 94103))
  .distinct()
  .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **10) What was the sum of all calls, average, minimum and maximum of the response times for calls?**

// COMMAND ----------

fire_tsDF
    .select(sum("NumAlarms").alias("suma"), avg("ResponseDelayedinMins").alias("media"), min("ResponseDelayedinMins").alias("Mínimo"), max("ResponseDelayedinMins").alias("Máximo"))
    .show()

// COMMAND ----------

// MAGIC %md
// MAGIC **11) What were all the different types of fire calls in 2018?**

// COMMAND ----------

fire_tsDF
  .select("CallType")
  .where($"CallType".isNotNull || (year($"IncidentDate") == 2018))
  .distinct()
  .show(32,false)

// COMMAND ----------

// MAGIC %md
// MAGIC **12) What months within the year 2018 saw the highest number of fire calls?**
// MAGIC 
// MAGIC January was the month with most calls and November was the month with less calls.

// COMMAND ----------

fire_tsDF
  .filter(year($"IncidentDate") === 2018)
  .groupBy(month($"IncidentDate"))
  .count()
  .orderBy(desc("count"))
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC **13) What week of the year in 2018 had the most fire calls?**
// MAGIC 
// MAGIC The weeks of the year in 2018 that had the most fire calls were the New Years' week (1) and 4th week oj July (25).

// COMMAND ----------

fire_tsDF
     .filter(year($"IncidentDate") === 2018)
     .groupBy(weekofyear($"IncidentDate"))
     .count()
     .orderBy(desc("count"))
     .show()

// COMMAND ----------

// MAGIC %md
// MAGIC **14) Which neighborhood in San Francisco generated the most fire calls in 2018?**
// MAGIC 
// MAGIC Tender Loin is the neighborhood in San Francisco generated the most fire calls in 2018.

// COMMAND ----------

fire_tsDF
    .filter(year($"IncidentDate") === 2018)
    .select("Neighborhood")
    .groupBy($"Neighborhood")
    .count()
    .orderBy(desc("count"))
    .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **15) What neighborhoods in San Francisco had the worst response time in 2018?**
// MAGIC 
// MAGIC West of Twin Peaks is the neighborhoods in San Francisco had the worst response time in 2018. If you living in West of Twin Peaks, the Fire Dept arrived in 754.083 mins.

// COMMAND ----------

fire_tsDF
    .select("Neighborhood", "ResponseDelayedinMins")
    .filter(year($"IncidentDate") === 2018)
     .orderBy(desc("ResponseDelayedinMins"))
    .show(1, false)

// COMMAND ----------

// MAGIC %md
// MAGIC **16) How can we use Parquet files to store data and read it back?**

// COMMAND ----------

fire_tsDF.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")

// COMMAND ----------

val file_parquetDF = spark.read.format("parquet").load("/tmp/fireServiceParquet/")

display(file_parquetDF.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC **17) How can we use Parquet SQL table to store data and read it back?**

// COMMAND ----------

fire_tsDF.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCallsScala")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM FireServiceCallsScala LIMIT 5
