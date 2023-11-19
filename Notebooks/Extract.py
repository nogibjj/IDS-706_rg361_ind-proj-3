# Databricks notebook source
#Importing Required Packages and Libraries
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pandas as pd

# COMMAND ----------

#To Validate if the DF has Data and is loaded as a dataframe
def validate_data(data):
    if not isinstance(data, pd.DataFrame):
        raise ValueError("Data should be a pandas DataFrame")
    if data.empty:
        raise ValueError("Data should not be empty")
    pass

# COMMAND ----------

def load_into_delta_lake(data, path):
    try:
        validate_data(data)

        # Convert pandas dataframe to spark dataframe
        spark = SparkSession.builder.getOrCreate()
        spark_df = spark.createDataFrame(data)

        # Write the DataFrame to a Delta Lake table (if the table already exists, overwrite it)
        spark_df.write.format("delta").mode("overwrite").saveAsTable(path)
    except ValueError as e:
        print(f"Invalid data: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# COMMAND ----------

# Use the function
try:
    data = pd.read_csv("https://github.com/Opensourcefordatascience/Data-sets/raw/master/automotive_data.csv")
    load_into_delta_lake(data, "delta_table_cars")
except Exception as e:
    print(f"An error occurred: {e}")
