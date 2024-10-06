'''
You are working on a data analysis project at Deloitte where you need to analyze a dataset containing information
about various cities. Your task is to calculate the population density of these cities, rounded to the nearest integer, and identify the cities with the minimum and maximum densities.
The population density should be calculated as (Population / Area).


The output should contain 'city', 'country', 'density'.

'''

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
result = cities_population.withColumn("density",round(col("population")/col("area"),2)).select(col("city"),col("country"),col("density")).filter(col("density").isNotNull())

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()