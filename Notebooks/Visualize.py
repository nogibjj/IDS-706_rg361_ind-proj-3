# Databricks notebook source
# Imnporting required packages and libraries
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

def validate_and_plot_data(result):
    # Validate result
    if result.rdd.isEmpty():
        raise ValueError("Query returned no results")

    # Convert the Spark DataFrame to a Pandas DataFrame
    result_pd = result.toPandas()
 
    # Validate pandas DataFrame
    if 'make' not in result_pd.columns or 'avg_hp' not in result_pd.columns or 'avg_price' not in result_pd.columns:
        raise ValueError("Expected columns are missing in the DataFrame")

    # Plot the relationship between Hp and Price by Make
    result_pd.plot(kind='scatter', x='avg_hp', y='avg_price')
    plt.title('Price vs Horsepower')
    plt.xlabel('Horsepower')
    plt.ylabel('Price')
    plt.savefig('../plot.png')
    plt.show()

# COMMAND ----------

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

try:
    result = spark.sql(
        """
        SELECT *
        FROM cars_transformed 
        """
    )
    validate_and_plot_data(result)
except AnalysisException as e:
    print(f"SQL query error: {e}")
except ValueError as e:
    print(f"Data validation error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
