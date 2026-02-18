-- Total Revenue
SELECT SUM(total_fare) AS total_revenue
FROM fact_rides;

-- Revenue by Month
SELECT year, month, SUM(total_fare) AS monthly_revenue
FROM fact_rides
GROUP BY year, month
ORDER BY year, month;

-- Top 5 Drivers by Revenue
SELECT driver_id, SUM(total_fare) AS driver_revenue
FROM fact_rides
GROUP BY driver_id
ORDER BY driver_revenue DESC
LIMIT 5;

-- Most Profitable Pickup Location
SELECT pickup_location, SUM(total_fare) AS location_revenue
FROM fact_rides
GROUP BY pickup_location
ORDER BY location_revenue DESC;

-- Average Ride Distance
SELECT AVG(distance_km) AS avg_distance
FROM fact_rides;