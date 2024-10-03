# Databricks notebook source
# MAGIC %fs ls /FileStore/rohit_databricks_accessKeys.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect AWS S3 Bucket using Access key and Secret Key 

# COMMAND ----------

# lets read the credentials csv file

aws_keys_df = (spark.read
               .format("csv")
               .options(header = True, inferschema = True)
               .load("/FileStore/rohit_databricks_accessKeys.csv")
               )

aws_keys_df.columns

# COMMAND ----------

access_key = aws_keys_df.select('Access key ID').take(1)[0]['Access key ID']
secret_key = aws_keys_df.select('Secret access key').take(1)[0]['Secret access key']

# COMMAND ----------

import urllib

encoded_secret_key = urllib.parse.quote(string = secret_key, safe="")

# COMMAND ----------

# specify the bucket name
AWS_S3_BUCKET = "rohit-bucket-databricks-capstone"



# this source url helps us attaching to our aws bucket
# this is the syntax we can use which is given in the databricks documentation.
SOURCE_URL = "s3a://%s:%s@%s" %(access_key, encoded_secret_key, AWS_S3_BUCKET)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

# autoloader is inferring schema wrongly, so we specify schema manually.

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Case Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Location Description", StringType(), True),
    StructField("Arrest", BooleanType(), True),
    StructField("Domestic", BooleanType(), True),
    StructField("Beat", IntegerType(), True),
    StructField("District", IntegerType(), True),
    StructField("Ward", IntegerType(), True),
    StructField("Community Area", IntegerType(), True),
    StructField("FBI Code", StringType(), True),
    StructField("X Coordinate", IntegerType(), True),
    StructField("Y Coordinate", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Updated On", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Location", StringType(), True)
])



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##  Data Ingestion into Bronze layer using autoloader
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###1.  let's work on Bronze layer 
# MAGIC

# COMMAND ----------

crime_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.region", "us-east-1")
        .option("header", "true")
        .schema(schema)
        .option("cloudFiles.useNotifications", "true") 
        .load(SOURCE_URL)  # Path to your S3 bucket or folder
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.1 Create a bronze database
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists capstone_bronze_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.2 Rename Columns
# MAGIC

# COMMAND ----------

# DataFrame contains column names with invalid characters (like spaces, commas, semicolons, braces, parentheses, or other special characters) that are not allowed when writing a streaming DataFrame to a Delta table.

# Clean column names by replacing invalid characters (e.g., spaces) with underscores
import re

clean_columns = [re.sub(r'[ ,;{}()\n\t=]', '_', col) for col in crime_df.columns]

# Assign clean column names to the DataFrame
crime_df = crime_df.toDF(*clean_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.3 Save bronze table

# COMMAND ----------

# Specify the target database and table

database_name = "capstone_bronze_db"
table_name = "crime_bronze_table"

# Define the path for the Delta table
delta_table_path = f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}"

# Write the streaming DataFrame to the Delta table

(crime_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/capstone/crimes_checkpoint")
    .option("path", delta_table_path)
    .trigger(availableNow = True)
    .table(f"{database_name}.{table_name}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##2.  lets work on silver layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####2.1 Check for schema validation and null values 

# COMMAND ----------

crime_df_bronze = (spark.readStream
      .table("capstone_bronze_db.crime_bronze_table")
)

display(crime_df_bronze)

# COMMAND ----------

from pyspark.sql.functions import col

# creating a static dataframe for joining and other purpose

static_crime_df = spark.table("capstone_bronze_db.crime_bronze_table")

static_invalid_rows_df = static_crime_df.filter(
    col("ID").isNull() |
    col("Case_Number").isNull() |
    col("Date").isNull() |
    col("Block").isNull() |
    col("IUCR").isNull() |
    col("Primary_Type").isNull() |
    col("Description").isNull() |
    col("Location_Description").isNull() |
    col("Arrest").isNull() |
    col("Domestic").isNull() |
    col("Beat").isNull() |
    col("District").isNull() |
    col("Ward").isNull() |
    col("Community_Area").isNull() |
    col("FBI_Code").isNull() |
    col("X_Coordinate").isNull() |
    col("Y_Coordinate").isNull() |
    col("Year").isNull() |
    col("Updated_On").isNull() |
    col("Latitude").isNull() |
    col("Longitude").isNull() |
    col("Location").isNull()
)

display(static_invalid_rows_df.count())

# we'll get 5302 rows.



# COMMAND ----------

# MAGIC %md
# MAGIC ####2.2 doing left-anti join to remove all invalid rows  

# COMMAND ----------

# the invalid rows are very less as compared to whole dataset.
# although it's not a good practice to drop all these rows but we do it here just for learning purpose.

# for joining a streaming dataframe, the right dataframe must be a static df, thats why we created a static df

silver_result_df = crime_df_bronze.join(static_invalid_rows_df, crime_df_bronze["ID"] == static_invalid_rows_df["ID"], "left_anti")


# COMMAND ----------

# MAGIC %md
# MAGIC ####2.3 Create silver database

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists capstone_silver_db

# COMMAND ----------

# MAGIC %md
# MAGIC ####2.4 Save silver table

# COMMAND ----------

# Specify the target database and table

database_name = "capstone_silver_db"
table_name = "crime_silver_table"

# Define the path for the Delta table
delta_table_path = f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}"

# Write the streaming DataFrame to the Delta table

(silver_result_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/capstone/crimes_checkpoint_silver")
    .option("path", delta_table_path)
    .trigger(availableNow = True)
    .table(f"{database_name}.{table_name}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##3  lets work on gold layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.1 first read data from silver table 

# COMMAND ----------

silver_df = (spark.readStream
      .table("capstone_silver_db.crime_silver_table")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.2 create a gold database

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists capstone_gold_db

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.3 create a gold table "crime_type_gold" 

# COMMAND ----------

# MAGIC %md
# MAGIC 3.3.1 checkpoint location for "crime_type_gold" table =   "dbfs:/mnt/capstone/crimes_checkpoint_gold"

# COMMAND ----------

from pyspark.sql import functions as F

crime_type_df = (
    silver_df
    .groupBy(F.col("Primary_Type").alias("Crime_Type"))
    .agg(
        F.count("*").alias("Total_Crimes"),  # Count of each Crime_Type
        F.sum(F.when(F.col("Arrest"), 1).otherwise(0)).alias("Arrested"),
        F.sum(F.when(F.col("Domestic"), 1).otherwise(0)).alias("Domestic")
    )
)

                 
display(crime_type_df)

# COMMAND ----------

# Specify the target database and table

database_name = "capstone_gold_db"
table_name = "crime_type_gold"

# Define the path for the Delta table
delta_table_path = f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}"

# Write the streaming DataFrame to the Delta table

(crime_type_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/capstone/crimes_checkpoint_gold")
    .outputMode("complete")
    .option("path", delta_table_path)
    .trigger(availableNow = True)
    .table(f"{database_name}.{table_name}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.4 create a gold table "crime_by_month_gold" 

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC 3.4.1 checkpoint location for "crime_by_month_gold" table =   "dbfs:/mnt/capstone/crimes_checkpoint_gold_1"

# COMMAND ----------

crime_by_month_df = (
    silver_df
    .withColumn("Timestamp", F.to_timestamp(F.col("Date"), "MM/dd/yyyy hh:mm:ss a"))  # Convert string to timestamp
    .withColumn("Month", F.date_format(F.col("Timestamp"), "MMMM"))  # Extract month in words
    .groupBy("Month")  # Group by month
    .agg(
        F.count("*").alias("Total_Crimes")  # Count total crimes for each month
    )
)

display(crime_by_month_df)


# COMMAND ----------

# Specify the target database and table

database_name = "capstone_gold_db"
table_name = "crime_by_month_gold"

# Define the path for the Delta table
delta_table_path = f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}"

# Write the streaming DataFrame to the Delta table

(crime_by_month_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/capstone/crimes_checkpoint_gold_1")
    .outputMode("complete")
    .option("path", delta_table_path)
    .trigger(availableNow = True)
    .table(f"{database_name}.{table_name}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.5 create a gold table "crime_district_gold" 

# COMMAND ----------

# MAGIC %md
# MAGIC 3.5.1 checkpoint location for "crime_district_gold" table = "dbfs:/mnt/capstone/crimes_checkpoint_gold_2"

# COMMAND ----------

crime_district_df = (
    silver_df
    .groupBy(F.col("District").alias("Crime_District"))
    .agg(
        F.count("*").alias("Total_Crimes"),  # Count of each Crime_Type
        F.sum(F.when(F.col("Domestic"), 1).otherwise(0)).alias("Domestic")
    )
)

display(crime_district_df)

# COMMAND ----------

# Specify the target database and table

database_name = "capstone_gold_db"
table_name = "crime_district_gold"

# Define the path for the Delta table
delta_table_path = f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}"

# Write the streaming DataFrame to the Delta table

(crime_district_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/capstone/crimes_checkpoint_gold_2")
    .outputMode("complete")
    .option("path", delta_table_path)
    .trigger(availableNow = True)
    .table(f"{database_name}.{table_name}")
)
