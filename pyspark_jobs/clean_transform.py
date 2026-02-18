from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, sum as spark_sum
import os

# Start Spark
spark = SparkSession.builder.appName("RideShare Analytics").getOrCreate()

# Base path
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_path, "data", "rides_raw.csv")

print("Reading CSV from:", csv_path)

# Read CSV
df = spark.read.csv(csv_path, header=True, inferSchema=True)

print("Total Rows:", df.count())

# Data Cleaning
df = df.dropDuplicates(["ride_id"])
df = df.filter(col("distance_km") > 0)
df = df.filter(col("fare_amount") > 0)

# Convert date
df = df.withColumn("ride_timestamp", to_timestamp("ride_date"))

# Extract year & month
df = df.withColumn("year", year("ride_timestamp"))
df = df.withColumn("month", month("ride_timestamp"))

# Calculate total_fare
df = df.withColumn("total_fare", col("fare_amount") * col("surge_multiplier"))

print("\nSample Data:")
df.show(5)

# Register as SQL table
df.createOrReplaceTempView("rides")

# --------------------------
# 1️⃣ Total Revenue
# --------------------------
print("\nTotal Revenue:")
spark.sql("""
SELECT SUM(total_fare) AS total_revenue
FROM rides
""").show()

# --------------------------
# 2️⃣ Monthly Revenue
# --------------------------
print("\nMonthly Revenue:")
spark.sql("""
SELECT month,
       SUM(total_fare) AS revenue
FROM rides
GROUP BY month
ORDER BY month
""").show()

# --------------------------
# 3️⃣ Top 5 Drivers
# --------------------------
print("\nTop 5 Drivers:")
spark.sql("""
SELECT driver_id,
       SUM(total_fare) AS revenue
FROM rides
GROUP BY driver_id
ORDER BY revenue DESC
LIMIT 5
""").show()

# --------------------------
# 4️⃣ Revenue by Location
# --------------------------
print("\nRevenue by Pickup Location:")
spark.sql("""
SELECT pickup_location,
       SUM(total_fare) AS revenue
FROM rides
GROUP BY pickup_location
ORDER BY revenue DESC
""").show()

spark.stop()