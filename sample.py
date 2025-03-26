from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Optimized Temporal Join") \
    .getOrCreate()

# Read input CSVs
address_df = spark.read.option("header", True).csv("/mnt/data/cust_address.csv")
mobile_df = spark.read.option("header", True).csv("/mnt/data/cust_mobileno.csv")
department_df = spark.read.option("header", True).csv("/mnt/data/cust_department.csv")

# Convert date columns to DateType
address_df = address_df.withColumn("eff_start_date", to_date("eff_start_date"))
address_df = address_df.withColumn("eff_end_date", to_date("eff_end_date"))

mobile_df = mobile_df.withColumn("eff_start_date", to_date("eff_start_date"))
mobile_df = mobile_df.withColumn("eff_end_date", to_date("eff_end_date"))

department_df = department_df.withColumn("eff_start_date", to_date("eff_start_date"))
department_df = department_df.withColumn("eff_end_date", to_date("eff_end_date"))

# Join logic based on overlapping date ranges and customer
# First join department and address using left join
join1 = department_df.alias("d").join(
    address_df.alias("a"),
    (col("d.customer") == col("a.customer")) &
    (col("a.eff_start_date") <= col("d.eff_end_date")) &
    (col("a.eff_end_date") >= col("d.eff_start_date")),
    "left"
)

# Then join with mobile using left join
final_df = join1.alias("da").join(
    mobile_df.alias("m"),
    (col("da.customer") == col("m.customer")) &
    (col("m.eff_start_date") <= col("da.eff_end_date")) &
    (col("m.eff_end_date") >= col("da.eff_start_date")),
    "left"
)

# Calculate the overlapping effective start and end dates
final_df = final_df.withColumn("eff_start_date", expr("GREATEST(d.eff_start_date, a.eff_start_date, m.eff_start_date)"))
final_df = final_df.withColumn("eff_end_date", expr("LEAST(d.eff_end_date, a.eff_end_date, m.eff_end_date)"))

# Select final fields
result = final_df.select(
    col("d.customer").alias("customer"),
    col("d.department"),
    col("a.address"),
    col("m.mobile_no"),
    col("eff_start_date"),
    col("eff_end_date"),
    col("d.active")  # assuming department is the driver
)

# Write output
result.write.mode("overwrite").option("header", True).csv("/mnt/data/final_result_output")
