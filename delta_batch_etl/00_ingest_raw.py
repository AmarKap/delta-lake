# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Data Generation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest data from a remote source into our source directory, `rawPath`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration
# MAGIC 
# MAGIC Before you run this cell, make sure to add a unique user name to the file
# MAGIC `includes/configuration`, e.g.
# MAGIC 
# MAGIC ```
# MAGIC username = "yourfirstname_yourlastname"
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Function
# MAGIC 
# MAGIC Run the following command to load the utility function, `retrieve_data`.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate User Table
# MAGIC 
# MAGIC Run the following cell to generate a User dimension table.

# COMMAND ----------

# MAGIC %run ./includes/user

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(classicPipelinePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Lab Data
# MAGIC 
# MAGIC Run this cell to prepare the data we will use for this Lab.

# COMMAND ----------

prepare_activity_data(landingPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion function
# MAGIC The `includes/utilities` file contains a function called `ingest_classic_data()`.
# MAGIC We will use this function to ingest an hour of data at a time.
# MAGIC This will simulate a Kafka feed.
# MAGIC 
# MAGIC Run this function here to ingest the first hour of data.
# MAGIC 
# MAGIC If successful, you should see the result, `True`.

# COMMAND ----------

ingest_classic_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data Directory
# MAGIC You should see that one file has landed in the Raw Data Directory.

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **EXERCISE:** Land five hours of data using the utility function,
# MAGIC `ingest_classic_data`.
# MAGIC 
# MAGIC 😎 **Note** the function can take a number of `hours` as an argument.

# COMMAND ----------

ingest_classic_data(hours = 5)

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print the Contents of the Raw Files
# MAGIC **EXERCISE**: Add the correct file paths to display the contents of the two raw files you loaded.

# COMMAND ----------

# TODO
print(dbutils.fs.head(rawPath + "2022-01-23-16-34-35.txt"))

# COMMAND ----------

# TODO
print(dbutils.fs.head(rawPath + "2022-01-23-16-35-41.txt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What do you notice about the data? (scroll down)