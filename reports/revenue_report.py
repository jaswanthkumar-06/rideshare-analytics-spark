import os
import pandas as pd
import matplotlib.pyplot as plt

# Get base path
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_path, "data", "rides_raw.csv")
reports_path = os.path.join(base_path, "reports")

print("Reading CSV from:", csv_path)

# Read CSV directly
df = pd.read_csv(csv_path)

print("Total Rows Loaded:", len(df))

# Calculate total_fare
df["total_fare"] = df["fare_amount"] * df["surge_multiplier"]

# -------------------
# 1️⃣ Monthly Revenue
# -------------------
df["month"] = pd.to_datetime(df["ride_date"]).dt.month
monthly = df.groupby("month")["total_fare"].sum().reset_index()

plt.figure()
plt.plot(monthly["month"], monthly["total_fare"])
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.savefig(os.path.join(reports_path, "monthly_revenue.png"))
plt.close()

# -------------------
# 2️⃣ Top Drivers
# -------------------
top_drivers = df.groupby("driver_id")["total_fare"].sum().sort_values(ascending=False).head(5)

plt.figure()
top_drivers.plot(kind="bar")
plt.title("Top 5 Drivers by Revenue")
plt.savefig(os.path.join(reports_path, "top_drivers.png"))
plt.close()

# -------------------
# 3️⃣ Location Revenue
# -------------------
location_revenue = df.groupby("pickup_location")["total_fare"].sum()

plt.figure()
location_revenue.plot(kind="bar")
plt.title("Revenue by Pickup Location")
plt.savefig(os.path.join(reports_path, "location_revenue.png"))
plt.close()

print("Graphs successfully saved in reports folder!")