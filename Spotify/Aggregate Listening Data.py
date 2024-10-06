'''
You're tasked with analyzing a Spotify-like dataset that captures user listening habits.
For each user, calculate the total listening time and the count of unique songs they've listened to. In the database duration values are displayed in seconds. Round the total listening duration to the nearest whole minute.

The output should contain three columns: 'user_id', 'total_listen_duration', and 'unique_song_count'.
'''

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
duration = listening_habits.groupBy(col("user_id")).agg(sum("listen_duration").alias("total_listen_duration"))

unique_count = listening_habits.filter(col("listen_duration") > 0).groupBy(col("user_id"),col("song_id")).agg(count(col("song_id")).alias("song_count")).filter(col("song_count") == 1).groupBy(col("user_id")).agg(sum(col("song_count")).alias("unique_song_count"))

result = unique_count.join(duration, unique_count.user_id == duration.user_id , "inner" ).drop(unique_count.user_id)
# To validate your solution, convert your final pySpark df to a pandas df
unique_count.toPandas()