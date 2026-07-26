from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Q3: Write a Spark command to read a CSV file located at "data/source.csv", 
# ensuring the first row is treated as a header and inferSchema is enabled.

spark = SparkSession.builder \
    .appName("Week6 Assignment") \
    .getOrCreate()

df = spark.read.csv(
    "week6_sample_data.csv",
    header=True,
    inferSchema=True
)

df.show()

# Q5: Given a DataFrame df, write a query to select the columns
#product_id and price where the category is 'Electronics'.

result = df.filter(col("category") == "Electronics") \
           .select("product_id", "price")

result.show()

# Q6: Write the code to "revise" a DataFrame by renaming the column old_name 
# to new_name and casting the price column from a String to a Double.

revised_df = df.withColumnRenamed("old_name", "new_name") \
               .withColumn("price", col("price").cast(DoubleType()))

revised_df.printSchema()
revised_df.show()

# Q8: Write a query to filter a DataFrame df_orders for rows 
# where the status is 'Completed' AND the amount is greater than 1000.

completed_orders = df.filter(
    (col("status") == "Completed") &
    (col("amount") > 1000)
)

completed_orders.show()

# Q10: Write a code snippet to add a new column final_price which is the base_price multiplied by 1.18 (18% tax).

price_df = df.withColumn(
    "final_price",
    col("base_price") * 1.18
)

price_df.show()

# Q12: Write the Spark command to load a Parquet file from "path/to/input", filter 
# out any rows where user_id is null, and save the result as a CSV at "path/to/output".

parquet_df = spark.read.parquet("path/to/input")

filtered_df = parquet_df.filter(col("user_id").isNotNull())

filtered_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("path/to/output")

# Q14: Write a query to filter a dataset for rows where the region is 'North' OR the priority is 'High'.

result = df.filter(
    (col("region") == "North") |
    (col("priority") == "High")
)

result.show()

spark.stop()