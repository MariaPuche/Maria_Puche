// Databricks notebook source
// MAGIC %md
// MAGIC In this notebook, we will focus on the following common relational operations:
// MAGIC - Unions and joins.
// MAGIC - Windowing.
// MAGIC - Modifications.
// MAGIC 
// MAGIC ## 1) CREATE DATAFRAMES

// COMMAND ----------

// MAGIC %md
// MAGIC To perform these DataFrame operations, we’ll first prepare some data. 
// MAGIC 
// MAGIC 1. Import two files and create two DataFrames, one for airport (airportsna) information and one for US flight delays (departureDelays).
// MAGIC 
// MAGIC 
// MAGIC 2. Using expr(), convert the delay and distance columns from STRING to INT.

// COMMAND ----------

// Import packages
import org.apache.spark.sql.functions._

// Set file paths
val delaysPath = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val airportsPath = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

// Obtain airports data set
val airports = spark.read
                    .option("header","true")            // la primera fila es el nombre de las columnas
                    .option("inferschema","true")      // Tienen schema
                    .option("delimiter","\t")          // las columnas se separan por \t
                    .csv(airportsPath)                 // entre parentesis se indica la ruta donde se encuentra el data set.
airports.createOrReplaceTempView("airports_na")

// Obtain departure Delays data set
val delays = spark.read
                   .option("header","true") // la primera fila contiene los nombres de las columnas
                   .csv(delaysPath)         // Entre parentesis aparece la ruta del archivo
                   .withColumn("delay", expr("CAST(delay as INT) as delay"))  // Convertir la columna delay a INT
                   .withColumn("distance", expr("CAST(distance as INT) as distance"))  // Convertir la columna distance a INT
delays.createOrReplaceTempView("departureDelays")

// COMMAND ----------

// MAGIC %md
// MAGIC 3 - Create a smaller table, foo, that we can focus on for our demo examples; it contains only information on three flights originating from Seattle (SEA) to the destination of San Francisco (SFO) for a small time range.

// COMMAND ----------

// Create temporary small table (there are only three rows)
val foo = delays.filter(
  expr(""" origin == 'SEA' AND destination == 'SFO' AND date like '01010%'               AND delay > 0 """))
foo.createOrReplaceTempView("foo")

// COMMAND ----------

// MAGIC %md
// MAGIC Now, we show the diferente dataframes. 

// COMMAND ----------

spark.sql("SELECT * FROM airports_na LIMIT 5").show()

// COMMAND ----------

spark.sql("SELECT * FROM departureDelays LIMIT 5").show()

// COMMAND ----------

spark.sql("SELECT * FROM foo").show()

// COMMAND ----------

// MAGIC %md
// MAGIC In the following sections, we will execute union, join and windowing examples with this data.
// MAGIC 
// MAGIC ## 2) UNIONS
// MAGIC Union two different DataFrames with the same schema together.

// COMMAND ----------

// Union two tables delays and foo
val bar = delays.union(foo)

// Create a temporary table
bar. createOrReplaceTempView("bar")

//Show the union (filtering fro SEA and SFO in a specific time range)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3) JOINS()
// MAGIC Join two dataframes or tables together. By default, a Spark SQL join is an inner join , with the options being inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti. 
// MAGIC 
// MAGIC The following exercise performs the default of an inner join between the airportsna and foo DataFrames. This table show the date, delay, distance and destination information from the foo DataFrame joined to the city and state information from the airports DataFrame.

// COMMAND ----------

// Join airportsna and foo
foo. join(airports.as('air),
         $"air.IATA" === $"origin")
    .select("City", "State", "date", "delay", "distance", "destination").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## WINDOW FUNCTION
// MAGIC 
// MAGIC Operate on a group of rows while still returning a single value for every input row.
// MAGIC 
// MAGIC Let's calculate the sum of Delay by flights originating from Seattle (SEA), San Francisco (SFO) and New York City (JFK) and going to a specific set of destination locations:

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS departureDelaysWindow; -- Delete table
// MAGIC 
// MAGIC CREATE TABLE departureDelaysWindow AS
// MAGIC SELECT origin, destination, sum(delay) AS TotalDelays
// MAGIC FROM departureDelays
// MAGIC WHERE origin IN ('SEA','SFO','JFK')
// MAGIC     AND  destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
// MAGIC GROUP BY origin, destination;
// MAGIC 
// MAGIC SELECT * FROM departureDelaysWindow;

// COMMAND ----------

// MAGIC %md
// MAGIC **What if for each of these origin airports you wanted to find the three destinations that experienced the most delays?**
// MAGIC 
// MAGIC We can ascertain that the destinatinations with the worst delays for the three origin cities were:
// MAGIC 
// MAGIC   •Seattle (SEA): San Francisco (SFO), Denver (DEN), and Chicago (ORD)
// MAGIC 
// MAGIC   • San Francisco (SFO): Los Angeles (LAX), Chicago (ORD), and New York (JFK)
// MAGIC 
// MAGIC   • New York (JFK): Los Angeles (LAX), San Francisco (SFO), and Atlanta (ATL)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT origin, destination, TotalDelays, rank 
// MAGIC  FROM ( 
// MAGIC  SELECT origin, destination, TotalDelays, dense_rank() 
// MAGIC  OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
// MAGIC  FROM departureDelaysWindow
// MAGIC  ) t 
// MAGIC  WHERE rank <= 3;

// COMMAND ----------

// MAGIC %md
// MAGIC ## MODIFICATIONS
// MAGIC You can modify DataFrames through operations that create new, different DataFrames, with different columns.

// COMMAND ----------

// Add new columns using WITHCOLUMN()
import org. apache.spark.sql.functions.expr

val foo2 = foo.withColumn("status",
                         expr ("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))

foo2.show()

// COMMAND ----------

// Drop delay columns using DROP()
val foo3 = foo2.drop("delay")
foo3.show()

// COMMAND ----------

// Rename a column using RENAME()
val foo4 = foo3.withColumnRenamed("status","flight_status") // Primero el nombre actual de la columna y segundo el nombre que le queremos poner.
foo4.show()

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Swap the columns for the rows (pivoting)
// MAGIC SELECT destination, CAST(SUBSTRING(date,0 ,2) AS int) AS month, delay
// MAGIC FROM departureDelays
// MAGIC WHERE origin = 'SEA';

// COMMAND ----------

// MAGIC %md
// MAGIC Pivoting allows you to place names in the month column (instead of 1 and 2 you can show Jan and Feb, respectively) as well as perform aggregate calculations (in this case average and max) on the delays by destination and month:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM ( 
// MAGIC   SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
// MAGIC   FROM departureDelays 
// MAGIC   WHERE origin = 'SEA')
// MAGIC PIVOT (CAST(AVG(delay) AS DECIMAL (4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay FOR month in (1 JAN, 2 FEB))
// MAGIC ORDER BY destination
