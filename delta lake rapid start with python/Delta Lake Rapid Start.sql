/*
# Notebook Goals

1. Use Delta Lake to create a new Delta table.

2. Convert an existing Parquet-based data lake table.

3. Differentiate between a batch update and an upsert to a Delta table.

4. Use Delta Lake Time Travel to view different versions of a Delta table.

5. Execute a MERGE command to upsert data into a Delta table.
*/

displayHTML("<img src ='files/tables/Delta_Lake_Rapid_Start_with_Python.png'>")

/*
# Configure Apache Spark to get optimal performance.

## These will include:

1. Specifying a database in which to work

2. Configuring the number of shuffle partitions to use
*/

username = "amar"
dbutils.widgets.text("username", username)
spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")
health_tracker = f"/dbacademy/{username}/DLRS/healthtracker/"

spark.conf.set("spark.sql.shuffle.partitions", 8)

/*
# Setup Data

1. Download the data to the driver
2. Verify the downloads
3. Move the data to the raw directory
4. Load the data as dataframe
*/

%sh

wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_1.json
wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2.json
wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2_late.json
wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_3.json

%sh ls

# Move the data to the raw directory
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_1.json", 
              health_tracker + "raw/health_tracker_data_2020_1.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2.json", 
              health_tracker + "raw/health_tracker_data_2020_2.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2_late.json", 
              health_tracker + "raw/health_tracker_data_2020_2_late.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_3.json", 
              health_tracker + "raw/health_tracker_data_2020_3.json")

# Load data into dataframe
file_path = health_tracker + "raw/health_tracker_data_2020_1.json"
 
health_tracker_data_2020_1_df = (
  spark.read
  .format("json")
  .load(file_path)
)

/*
# Visualize data

## Display the data

- Strictly speaking, this is not part of the ETL process, but displaying the data gives us a look at the data that we are working with. 

- We note a few phenomena in the data:

  1. Sensor anomalies - Sensors cannot record negative heart rates, so any negative values in the data are anomalies.

  2. Wake/Sleep cycle - We notice that users have a consistent wake/sleep cycle alternating between steady high and low heart rates.

  3. Elevated activity - Some users have irregular periods of high activity.
*/

display(health_tracker_data_2020_1_df)

/*
# Create a Parquet Table

 - Throughout this example, we'll be writing files to the root location of the Databricks File System (DBFS). In general, best practice is to write files to your cloud object storage. We use DBFS root here for demonstration purposes.
 
 - Step 1: Remove files in the /dbacademy/DLRS/healthtracker/processed directory. Next, we remove the files in the /dbacademy/DLRS/healthtracker/processed directory. This step will make the notebook idempotent. In other words, it could be run more than once without throwing errors or introducing extra files.
*/

dbutils.fs.rm(health_tracker + "processed", recurse=True)

/*
# Transform the data 

## We will perform data engineering on the data with the following transformations:

1. Use the from_unixtime Spark SQL function to transform the unix timestamp into a time string

2. Cast the time column to type timestamp to replace the column time

3. Cast the time column to type date to create the column dte

4. Select the columns in the order in which we would like them to be writte

As this is a process that we will perform on each dataset as it is loaded we compose a function to perform the necessary transformations. This function, process_health_tracker_data, can be reused each time.
*/

from pyspark.sql.functions import col, from_unixtime
 
def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "heartrate", "name", "p_device_id")
    )
  
processedDF = process_health_tracker_data(health_tracker_data_2020_1_df)

(processedDF.write
 .mode("overwrite")
 .format("parquet")
 .partitionBy("p_device_id")
 .save(health_tracker + "processed"))

%sql 

DROP TABLE IF EXISTS health_tracker_processed;

CREATE TABLE health_tracker_processed                        
USING PARQUET                
LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# If you check the count it will be zero
health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

%sql

MSCK REPAIR TABLE health_tracker_processed

# Count the records in the health_tracker_processed table after repair
health_tracker_processed.count()

%sql
DESCRIBE DETAIL health_tracker_processed

from delta.tables import DeltaTable

parquet_table = f"parquet.`{health_tracker}processed`"
partitioning_scheme = "p_device_id int"

DeltaTable.convertToDelta(spark, parquet_table, partitioning_scheme)

%sql
DROP TABLE IF EXISTS health_tracker_processed;

CREATE TABLE health_tracker_processed
USING DELTA
LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

%sql
DESCRIBE DETAIL health_tracker_processed

health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# Remove files in the health_tracker_user_analytics directory This step will make the notebook idempotent.
dbutils.fs.rm(health_tracker + "gold/health_tracker_user_analytics",
              recurse=True)

# The subquery used to define the table aggregates the health_tracker_processed Delta table by device, and computes summary statistics.

from pyspark.sql.functions import col, avg, max, stddev

health_tracker_gold_user_analytics = (
  health_tracker_processed
  .groupby("p_device_id")
  .agg(avg(col("heartrate")).alias("avg_heartrate"),
       max(col("heartrate")).alias("max_heartrate"),
       stddev(col("heartrate")).alias("stddev_heartrate"))
)

(health_tracker_gold_user_analytics.write
 .format("delta")
 .mode("overwrite")
 .save(health_tracker + "gold/health_tracker_user_analytics"))

%sql

DROP TABLE IF EXISTS health_tracker_gold_user_analytics;

CREATE TABLE health_tracker_gold_user_analytics
USING DELTA
LOCATION "/dbacademy/$username/DLRS/healthtracker/gold/health_tracker_user_analytics"

/*
At Delta table creation, the Delta files in Object Storage define the schema, partitioning, and table properties. For this reason, it is not necessary to specify any of these when registering the table with the Metastore. Furthermore, NO TABLE REPAIR IS REQUIRED. The transaction log stored with the Delta files contains all the metadata needed for an immediate query.
*/

/*
# Appending files to an existing Delta table

 Two patterns for modifying existing Delta tables:

- Appending files to an existing directory of Delta files

- Merging a set of updates and insertions

## Load the next month of data

Here, we append the next month of records. We begin by loading the data from the file health_tracker_data_2020_2.json, using the .format("json") option as before.
*/

file_path = health_tracker + "raw/health_tracker_data_2020_2.json"
 
health_tracker_data_2020_2_df = (
  spark.read
  .format("json")
  .load(file_path)
)

/*
# Transform the data

We perform the same data engineering on the data:

1. use the from_unixtime Spark SQL function to transform the unixtime into a time string

2. cast the time column to type timestamp to replace the column time

3. cast the time column to type date to create the column dte

This is done using the process_health_tracker_data function we defined previously.
*/

processedDF = process_health_tracker_data(health_tracker_data_2020_2_df)

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

# Here, we query the data as of version 0, that is, the initial conversion of the table from Parquet.
(spark.read
 .option("versionAsOf", 0)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

health_tracker_processed.count()

from pyspark.sql.functions import count

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .groupby("p_device_id")
  .agg(count("*"))
)

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .where(col("p_device_id").isin([3,4]))
)

# First, we create a temporary view for the Broken Readings in the health_tracker_processed table.

broken_readings = (
  health_tracker_processed
  .select(col("heartrate"), col("dte"))
  .where(col("heartrate") < 0)
  .groupby("dte")
  .agg(count("heartrate"))
  .orderBy("dte")
)
 
broken_readings.createOrReplaceTempView("broken_readings")

%sql

SELECT * FROM broken_readings

%sql 
SELECT SUM(`count(heartrate)`) FROM broken_readings

/*
# Repair Records with an Upsert
 
- We identified two issues with the health_tracker_processed table:

  1. There were 72 missing records

  2. There were 67 records with broken readings

- We will now repair the table by modifying the health_tracker_processed table.

## There are two patterns for modifying existing Delta tables. 

   1. appending files to an existing directory of Delta files

   2. merging a set of updates and insertions
   
- When upserting into an existing Delta table, use Spark SQL to perform the merge from another registered table or view. The Transaction Log records the transaction, and the Metastore immediately reflects the changes.

- The merge appends both the new/inserted files and the files containing the updates to the Delta file directory. The transaction log tells the Delta reader which file to use for each record.
*/

# Create a DataFrame interpolating broken values

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead
 
dteWindow = Window.partitionBy("p_device_id").orderBy("dte")
 
interpolatedDF = (
  spark.read
  .table("health_tracker_processed")
  .select(col("dte"),
          col("time"),
          col("heartrate"),
          lag(col("heartrate")).over(dteWindow).alias("prev_amt"),
          lead(col("heartrate")).over(dteWindow).alias("next_amt"),
          col("name"),
          col("p_device_id"))
)

# Create a DataFrame of updates

updatesDF = (
  interpolatedDF
  .where(col("heartrate") < 0)
  .select(col("dte"),
          col("time"),
          ((col("prev_amt") + col("next_amt"))/2).alias("heartrate"),
          col("name"),
          col("p_device_id"))
)

# We use the .printSchema() function to view the schema of the health_tracker_processed table.

health_tracker_processed.printSchema()
updatesDF.printSchema()

# Perform a .count() on the updatesDF view. It should have the same number of records as the SUM performed on the broken_readings view.

updatesDF.count()

# Load the late-arriving data

file_path = health_tracker + "raw/health_tracker_data_2020_2_late.json"
 
health_tracker_data_2020_2_late_df = (
  spark.read
  .format("json")
  .load(file_path)
)

/*
## Transform the data

- In addition to updating the broken records, we wish to add this late-arriving data. We begin by preparing another temporary view with the appropriate transformations:

1. Use the from_unixtime Spark SQL function to transform the unixtime into a time string

2. Cast the time column to type timestamp to replace the column time

3. Cast the time column to type date to create the column dte
*/

insertsDF = process_health_tracker_data(health_tracker_data_2020_2_late_df)

# View the schema of the inserts DataFrame

insertsDF.printSchema()

# Create the union DataFrame. Finally, we prepare the upsertsDF that consists of all the records in both the updatesDF and the insertsDF. We use the DataFrame .union() command to create the view.

upsertsDF = updatesDF.union(insertsDF)

# View the schema
upsertsDF.printSchema()

processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")

update_match = """
  health_tracker.time = upserts.time 
  AND 
  health_tracker.p_device_id = upserts.p_device_id
"""

update = { "heartrate" : "upserts.heartrate" }

insert = {
  "p_device_id" : "upserts.p_device_id",
  "heartrate" : "upserts.heartrate",
  "name" : "upserts.name",
  "time" : "upserts.time",
  "dte" : "upserts.dte"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

(spark.read
 .option("versionAsOf", 1)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# Count the most recent version

health_tracker_processed.count()

# The .history() Delta table command provides provenance information, including the operation, user, and so on, for each action performed on a table.

display(processedDeltaTable.history())

/*
# Perform a Second Upsert

- We performed an upsert to the health_tracker_processed table, simultaneously:

1. updated records containing broken readings

2. Inserting the late-arriving data

In doing so, we added more broken readings!

It is not necessary to redefine the view `broken_readings`. The view is simply a pointer to the query, as opposed to being the actual data, and automatically pulls the latest correct number of broken readings from the data in our health_tracker_processed Delta table.
*/

%sql 

SELECT SUM(`count(heartrate)`) FROM broken_readings

/*
Verify that these are new broken readings

Let’s query the `broken_readings` with a WHERE clause to verify that these are indeed new broken readings introduced by inserting the late-arriving data. 

Note that there are no broken readings before ‘2020-02-25’.
*/

%sql 

SELECT SUM(`count(heartrate)`) FROM broken_readings WHERE dte < '2020-02-25'

# Perform a .count() on the updatesDF view. 
updatesDF.count()

# Once more, we upsert into the health_tracker_processed Table using the DeltaTable command .merge().

upsertsDF = updatesDF

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

%sql

SELECT SUM(`count(heartrate)`) FROM broken_readings

/*
# Evolution of data being ingested

It is not uncommon that schema of the data being ingested into the EDSS will evolve over time. In this case, the simulated health tracker device has a new version available and the data being transmitted now contains an additional field indicating which type of device is being used.

Here is a sample of the data we will be using. Each line is a string representing a valid JSON object and is similar to the kind of string that would be passed by a Kafka stream processing server.

`{"device_id":0,"heartrate":57.6447293596,"name":"Deborah Powell","time":1.5830208E9,"device_type":"version 2"}`
`{"device_id":0,"heartrate":57.6175546013,"name":"Deborah Powell","time":1.5830244E9,"device_type":"version 2"}`
`{"device_id":0,"heartrate":57.8486376876,"name":"Deborah Powell","time":1.583028E9,"device_type":"version 2"}`
`{"device_id":0,"heartrate":57.8821378637,"name":"Deborah Powell","time":1.5830316E9,"device_type":"version 2"}`
`{"device_id":0,"heartrate":59.0531490807,"name":"Deborah Powell","time":1.5830352E9,"device_type":"version 2"}`

The data now has the following schema:

`name: string`
`heartrate: double`
`device_id: long`
`time: long`
`device_type: string`
*/

# We begin by loading the data from the file health_tracker_data_2020_3.json, using the .format("json") option as before.

file_path = health_tracker + "raw/health_tracker_data_2020_3.json"

health_tracker_data_2020_3_df = (
  spark.read
  .format("json")
  .load(file_path)
)

/*
# Transform the data

### We perform the same data engineering on the data:

1. Use the from_unixtime Spark SQL function to transform the unix timestamp into a time string

2. Cast the time column to type timestamp to replace the column time

3. Cast the time column to type date to create the column dte

Note that we redefine the function process_health_tracker_data to accommodate the new schema.
*/

def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "device_type", "heartrate", "name", "p_device_id")
    )
  
processedDF = process_health_tracker_data(health_tracker_data_2020_3_df)

# Append using .mode("append"). 

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

/*
When we try to run the command above, we receive the error shown below because there is a mismatch between the table and data schemas.

## What Is schema enforcement?

Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by rejecting writes to a table that do not match the table’s schema. It checks to see whether each column in data inserted into the table is on its list of expected columns and rejects any writes with columns that aren’t on the list or with data type mismatches.

## Appending files to an existing Delta table with schema evolution

In this case, we would like our table to accept the new schema and add the data to the table.

What Is schema evolution?

Schema evolution is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time. Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.
*/

# append using .mode("append") & option mergeSchema = True 

(processedDF.write
 .mode("append")
 .option("mergeSchema", True)
 .format("delta")
 .save(health_tracker + "processed"))

# When we look at the current version, we expect to see three months of data, five device measurements, 24 hours a day for (31 + 29 + 31) days, or 10920 records. 

health_tracker_processed.count()

/*
# Delete Data and Recover Lost Data

### Delete all records associated with a user

Under the European Union General Data Protection Regulation (GDPR) and the California Consumer Privacy Act (CCPA), a user of the health tracker device has the right to request that their data be expunged from the system. We might simply do this by deleting all records associated with that user's device id.
*/

# Here, we use the DELETE Spark SQL command to remove all records from the health_tracker_processed table that match the given predicate.

processedDeltaTable.delete("p_device_id = 4")

/*
# Recover lost data

Previously, we deleted all records from the health_tracker_processed table for the health tracker device with id 4. Suppose that the user did not wish to remove all of their data, but merely to have their name scrubbed from the system. In this case, we use the Time Travel capability of Delta Lake to recover everything but the user’s name.
*/

# We prepare a view for upserting using Time Travel to recover the missing records. 

# Note that we have replaced the entire name column with the value NULL.

from pyspark.sql.functions import lit

upsertsDF = (
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
  .select("dte", "time", "device_type", "heartrate", lit(None).alias("name"), "p_device_id")
)

# Once more, we upsert into the health_tracker_processed table using the  Delta table command .merge().

# Note that it is necessary to define 1) the reference to the Delta table and 2) the insert logic because the schema has changed.

processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")

insert = {
  "dte" : "upserts.dte",
  "time" : "upserts.time",
  "device_type" : "upserts.device_type",
  "heartrate" : "upserts.heartrate",
  "name" : "upserts.name",
  "p_device_id" : "upserts.p_device_id"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# When we look at the current version, we expect to see three months of data, five device measurements, 24 hours a day for (31 + 29 + 31) days, or 10920 records.

health_tracker_processed.count()

# We query the health_tracker_processed table to demonstrate that the name associated with device 4 has indeed been removed.

display(health_tracker_processed.where("p_device_id = 4"))

/*
# Maintain Compliance with a Vacuum Operation

Due to the power of the Delta Lake Time Travel feature, we are not yet in compliance as the table could simply be queried against an earlier version to identify the name of the user associated with device 4.
*/

# We query the health_tracker_processed table against an earlier version to demonstrate that it is still possible to retrieve the name associated with device 4.

display(
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)

# The VACUUM Spark SQL command can be used to solve this problem. The VACUUM command recursively vacuums directories associated with the Delta table and removes files that are no longer in the latest state of the transaction log for that table and that are older than a retention threshold. The default threshold is 7 days. 

processedDeltaTable.vacuum(0)

/*
## Delta table retention period

When we run this command, we receive the error. The default threshold is in place to prevent corruption of the Delta table.

IllegalArgumentException: requirement failed: Are you sure you would like to vacuum files with such a low retention period? If you have writers that are currently writing to this table, there is a risk that you may corrupt the state of your Delta table.

If you are certain that there are no operations being performed on this table, such as insert/upsert/delete/optimize, then you may turn off this check by setting: spark.databricks.delta.retentionDurationCheck.enabled = false

If you are not sure, please use a value not less than "168 hours".
*/

# To demonstrate the VACUUM command, we set our retention period to 0 hours to be able to remove the questionable files now. This is typically not a best practice and in fact, there are safeguards in place to prevent this operation from being performed.

# For demonstration purposes, we will set Delta to allow this operation.

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

processedDeltaTable.vacuum(0)

# Now when we attempt to query an earlier version, an error is thrown.

# This error indicates that we are not able to query data from this earlier version because the files have been expunged from the system.

display(
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)

/*
# We did this through the following steps:

1. We converted an existing Parquet-based data lake table to a Delta table, health_tracker_processed.

2. We performed a batch upload of new data to this table.

3. We used Apache Spark to identify broken and missing records in this table.

4. We used Delta Lake’s upsert functionality, where we updated broken records and inserted missing records.

5. We evolved the schema of the Delta table.

6. We used Delta Lake’s Time Travel feature to scrub the personal data of a user intelligently.

- Additionally, we used Delta Lake to create an aggregate table, health_tracker_user_analytics, downstream from the health_tracker_processed table.
*/