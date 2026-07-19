from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, min, max, mean, sum
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("Week5 Assignment") \
    .getOrCreate()

df = spark.read.csv(
    "sample_data.csv",
    header=True,
    inferSchema=True
)

# Q3: Write a code snippet to remove all duplicate rows from a DataFrame
# based on a specific set of columns: user_id and transaction_date.

print("Rows before removing duplicates:", df.count())

df_no_duplicates = df.dropDuplicates(
    ["user_id", "transaction_date"]
)

print("Rows after removing duplicates:", df_no_duplicates.count())

df_no_duplicates.show(truncate=False)

# Q4: Given a DataFrame df_sales, write a query to filter for rows where the region
# is 'West' and then group by product_category to find the average sale_amount.

west_sales = df.filter(df["region"] == "West")

result = west_sales.groupBy("product_category") \
                   .agg(avg("sale_amount").alias("average_sale_amount"))

result.show()


# Q 5 : What is the difference between .na.drop() and .na.fill()?
# Provide a code example of filling null values in a status column with the string 'Unknown'.

filled_df = df.na.fill({"status": "Unknown"})

filled_df.show(truncate=False)
filled_df.select("user_id", "status").show()

# Q6: Write a query to find the total count of records for each city in a DataFrame, 
# but only for cities where the count is greater than 100.

city_count = df.groupBy("city").count()

result = city_count.filter(city_count["count"] > 100)

result.show()

# Q8: Write a Spark command to filter a dataset for rows where the age is between 18 and 30 
# (inclusive) and the subscription is 'Premium'.

premium_users = df.filter(
    (df["age"].between(18, 30)) &
    (df["subscription"] == "Premium")
)

premium_users.show(truncate=False)

# Q10: Write the code to revise a column named raw_timestamp by 
# casting it to a TimestampType and renaming it to event_time.

timestamp_df = df.withColumn(
    "event_time",
    col("raw_timestamp").cast(TimestampType())
).drop("raw_timestamp")

timestamp_df.show(truncate=False)
timestamp_df.select("user_id", "event_time").show(truncate=False)

# Q12: Write a code snippet that identifies and removes rows where the 
# email column contains null values OR the username is an empty string.

clean_df = df.filter(
    col("email").isNotNull() &
    (col("username") != "")
)

clean_df.show(truncate=False)
print("Rows before cleaning:", df.count())
print("Rows after cleaning:", clean_df.count())

# Q13: How do you use the .agg() function to calculate multiple 
# statistics at once, such as the min, max, and mean of the price column?

result = df.agg(
    min("price").alias("Minimum Price"),
    max("price").alias("Maximum Price"),
    mean("price").alias("Average Price")
)

result.show()

# Q15: Write a final processing pipeline that:
# 1.	Filters out duplicates.
# 2.	Fills null prices with 0.
# 3.	Groups by store_id to calculate total revenue.

pipeline_df = (
    df.dropDuplicates(["user_id", "transaction_date"])
      .na.fill({"price": 0})
)

result = pipeline_df.groupBy("store_id") \
                    .agg(sum("price").alias("total_revenue"))

result.show()

# Stop Spark Session
spark.stop()


