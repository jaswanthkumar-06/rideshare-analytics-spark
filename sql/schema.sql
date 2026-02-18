CREATE TABLE fact_rides (
    ride_id INT PRIMARY KEY,
    driver_id INT,
    customer_id INT,
    pickup_location VARCHAR(100),
    drop_location VARCHAR(100),
    distance_km FLOAT,
    ride_duration INT,
    total_fare FLOAT,
    ride_timestamp TIMESTAMP,
    year INT,
    month INT,
    day INT
);

CREATE TABLE dim_driver (
    driver_id INT PRIMARY KEY
);

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY
);

CREATE TABLE dim_location (
    location_name VARCHAR(100) PRIMARY KEY
);