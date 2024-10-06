'''
Market penetration is an important metric for understanding Spotify's performance and growth potential in different regions.
You are part of the analytics team at Spotify and are tasked with calculating the active user penetration rate in specific countries.


For this task, 'active_users' are defined based on the  following criterias:


last_active_date: The user must have interacted with Spotify within the last 30 days.
•    sessions: The user must have engaged with Spotify for at least 5 sessions.
•    listening_hours: The user must have spent at least 10 hours listening on Spotify.


Based on the condition above, calculate the active 'user_penetration_rate' by using the following formula.


•    Active User Penetration Rate = (Number of Active Spotify Users in the Country / Total users in the Country)


Total Population of the country is based on both active and non-active users.
​
The output should contain 'country' and 'active_user_penetration_rate' rounded to 2 decimals.


Let's assume the current_day is 2024-01-31.
'''

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
df = penetration_analysis.filter("""last_active_date > "2024-01-01" and sessions >= 5 and listening_hours >= 10 """ ).groupBy("country").agg(count(col("user_id")).alias("active_users"))

df1 = penetration_analysis.groupBy("country").agg(count(col("user_id")).alias("total_user"))

result = df.join(df1,df.country == df1.country,"inner").drop(df.country)

final_result = result.withColumn("active_user_penetration_rate",round(col("active_users")/col("total_user"),2)).select(col("country"), col("active_user_penetration_rate"))
# To validate your solution, convert your final pySpark df to a pandas df
final_result.toPandas()