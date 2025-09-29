# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment 1: Working with Spark DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Concept Check
# MAGIC * Explain the below in 3-5 sentences
# MAGIC * 5 points


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Spark's distributed architecture and why is it so fast say as compared to Hadoop
# MAGIC * 1 point

# Spark is a distributed system that runs on many machines at the same time.
# It keeps most data in RAM instead of writing to disk after every step like Hadoop.
# Because of this, Spark can finish tasks much faster than Hadoop mapreduce.
# It also uses an optimized plan to reduce extra work and save time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 RDD Vs Data Frame Vs Data Set
# MAGIC * 1 point

# RDD is Spark’s basic distributed collection without a schema.
# DataFrame looks like a table with rows and columns and it is fast and easy to use with SQL.
# Dataset is like a typed DataFrame that gives compile-time safety.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Spark Lazy Loading
# MAGIC * 1 point

# Spark uses lazy evaluation.
# Transformations only build a plan and do not run immediately.
# When an action like count() or show() is called, Spark optimizes the whole plan and then executes it.
# This saves work and improves speed because unnecessary steps can be removed.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 What is Delta and how does it help big data processing ?
# MAGIC * 1 point

# Delta Lake is a storage format that makes big data tables reliable.
# It gives ACID guarantees, so data stays correct even with many users writing at the same time.
# It also supports schema changes and time travel to see older versions.
# Because of this, both batch and streaming jobs can use the same data easily.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 What is data modeling and why should you care ?
# MAGIC * 1 point

# Data modeling is the process of designing how data is stored and related.
# It creates tables and relationships that match business needs.
# Good modeling makes queries faster, improves quality, and avoids errors.
# In practice, it is important because clean structure leads to better insights and machine learning results.

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Read data
# MAGIC
# MAGIC Put the data from Google drive into a volume and set the {data-root} to the path of that volume.  You should have permission to do that in your workspace.  Here is documentation on how to create a volume: https://docs.databricks.com/aws/en/volumes/utility-commands
# MAGIC * File will be on google drive in https://drive.google.com/drive/folders/1Wps0LoyMWNc00g7GLpk_UaARCdNxBBz5?usp=drive_link
# MAGIC * Data set location: {data-root}/people10m
# MAGIC     * people10m-file1.parquet
# MAGIC * 10 points

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Read data in parquet format from given location ito a spark data frame
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Examine Schema
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Examine stats to 
# MAGIC * check data distribution of each column, 
# MAGIC * examine nulls, ranges etc
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Select & Filter
# MAGIC * Use the DataFrame APIs select and filter methods
# MAGIC * select the following fields firstName,middleName,lastName,birthDate,gender
# MAGIC * filter rows where gender is Female/F and birth year > 1990
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Query and Visualize
# MAGIC * How many women were named Mary in each year?
# MAGIC * Use an appropriate visualization to display your query result
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Compare
# MAGIC * Compare popularity of two names from 1990 - Donna & Dorothy
# MAGIC * popularity is defined as #occurances in that year
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Use agg & limit
# MAGIC * Create a DataFrame called top10FemaleFirstNamesDF that contains the 10 most common female first names out of the people data set.
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 Create a temporary view & query it
# MAGIC * From the original dataframe of all the data
# MAGIC * Run the same query from before this time using ql and compare counts - How many women were named Mary in each year?
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9 Persist data on disk
# MAGIC * Create a database <your user name>_Assignment1
# MAGIC * Persist all the data in csv format into a table called people_csv
# MAGIC * Persist all the data in delta format into a table called people_delta
# MAGIC * 1 point

# COMMAND ----------

spark.sql('CREATE CATALOG IF NOT EXISTS cscie103_catalog')
spark.sql('USE CATALOG cscie103_catalog')
spark.sql('CREATE SCHEMA IF NOT EXISTS assignment_01')
spark.sql('USE cscie103_catalog.assignment_01')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.10 Read data formats
# MAGIC * Read data from people_delta table
# MAGIC * Display schema
# MAGIC * Check counts on both
# MAGIC * Calculate average salary & min/max salary using built-in functions
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Joins 
# MAGIC * 5 points

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Load data 
# MAGIC * File will be on google drive in https://drive.google.com/drive/folders/1Wps0LoyMWNc00g7GLpk_UaARCdNxBBz5?usp=drive_link
# MAGIC * Data set location: {data-root}/names-1880-2024
# MAGIC   * names_file1.parquet
# MAGIC * /mnt/training/ssn/names-1880-2016.parquet/
# MAGIC * Perform a distinct count on first name on both data frames
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Inner Join 
# MAGIC * the 2 dataframes on first name
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Data Cleanse
# MAGIC * Convert negative salaries to positive numbers
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Data Transform
# MAGIC * assume all salaries under $20,000 represent bad rows and filter them out.
# MAGIC * categorize each person's salary into $10K groups.
# MAGIC   * A salary of 23,000 should report a value of "2".
# MAGIC   * A salary of 57,400 should report a value of "6".
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Concept Q
# MAGIC * Write the difference betweem the various join options
# MAGIC * Can you join on multiple conditions
# MAGIC * 1 point

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Semi-Structured Data
# MAGIC * Ex. JSON, XML
# MAGIC * 10 points

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Read JSON
# MAGIC
# MAGIC * File will be on google drive in https://drive.google.com/drive/folders/1Wps0LoyMWNc00g7GLpk_UaARCdNxBBz5?usp=drive_link
# MAGIC * Data set location: {data-root}
# MAGIC   * databricks-blog.json
# MAGIC * PrintSchema
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Access fields
# MAGIC * author, categories, title
# MAGIC * nested date fields individually
# MAGIC * Hint: select("dates.publishedOn")
# MAGIC * add a new column publishedOn at the top level and populate it from the nested date sub-field
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Date time fields
# MAGIC * cast publishedOn to timestamp using date_format
# MAGIC * filter on published year of 2013
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Array data
# MAGIC * Select the first author in the array aauthors as the primaryAuthor
# MAGIC * Hint: select("col(authors)[0]")
# MAGIC * 1 point

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Explode
# MAGIC * Split single row to multiple fields (for authors)
# MAGIC * Show title & each author on different rows
# MAGIC * Hint: select(explode(col("authors")).alias("Author")
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Search
# MAGIC * Identify all the articles written or co-written by Michael Armbrust.
# MAGIC * 1 point

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Count
# MAGIC * Count how many times each category is referenced in the Databricks blog.
# MAGIC * 2 point

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.8 Concept Q: 
# MAGIC * How will you handle multi-line json data
# MAGIC * 1 point

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.9 Concept Q:
# MAGIC * If your column contains json data, how will you parse it (which function) while reading
# MAGIC * How will you save data as json
# MAGIC * 1 point