CREATE OR REPLACE TABLE `silken-forest-466023-a2.uber_data_engineering.tbl_analytics` AS (
SELECT 
f.trip_id,
f.VendorID,
d.tpep_dropoff_datetime,
d.tpep_pickup_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pick.pickup_latitude,
pick.pickup_longitude,
dro.dropoff_latitude,
dro.dropoff_longitude,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM 
`silken-forest-466023-a2.uber_data_engineering.fact_table` f
JOIN `silken-forest-466023-a2.uber_data_engineering.datetime_dim` d ON f.datetime_id=d.datetime_id
JOIN `silken-forest-466023-a2.uber_data_engineering.passenger_count_dim` p ON p.passenger_count_id=f.passenger_count_id
JOIN `silken-forest-466023-a2.uber_data_engineering.trip_distance_dim` t ON t.trip_distance_id = f.trip_distance_id
JOIN `silken-forest-466023-a2.uber_data_engineering.rate_code_dim` r ON r.rate_code_id=f.rate_code_id
JOIN `silken-forest-466023-a2.uber_data_engineering.pickup_location_dim` pick ON pick.pickup_location_id=f.pickup_location_id
JOIN `silken-forest-466023-a2.uber_data_engineering.dropoff_location_dim` dro ON dro.dropoff_location_id=f.dropoff_location_id
JOIN `silken-forest-466023-a2.uber_data_engineering.payment_type_dim` pay ON pay.payment_type_id=f.payment_type_id)
;

--Top 10 pickup locations based on the number of trips
SELECT pickup_location_id, COUNT(*) AS trip_count
FROM `silken-forest-466023-a2.uber_data_engineering.fact_table`
GROUP BY pickup_location_id
ORDER BY trip_count DESC
LIMIT 10;

--Total number of trips by passenger count
SELECT passenger_count, count(*) AS num_trips
FROM `silken-forest-466023-a2.uber_data_engineering.fact_table` f
JOIN `silken-forest-466023-a2.uber_data_engineering.passenger_count_dim` p ON p.passenger_count_id=f.passenger_count_id 
GROUP BY p.passenger_count
ORDER BY num_trips DESC;

--Average fare amount by hour of the day
SELECT dt.pick_hour, ROUND(AVG(f.fare_amount),2) AS avg_fare_amt
FROM `silken-forest-466023-a2.uber_data_engineering.fact_table` f
JOIN `silken-forest-466023-a2.uber_data_engineering.datetime_dim` dt
ON dt.datetime_id = f.datetime_id
GROUP BY dt.pick_hour
ORDER BY dt.pick_hour;