# Homework 3

in the above `data_loaders` and `data_exporters` you can find the Mage code used to get the data and load it to GCS bucket to be used in Bigquery.
Below are the queries used to answer the questions

### Q1 query
```SELECT count(*) FROM `master-antenna-414015.green_taxi_nyc.taxi_rides` ```

### Q2 query
```SELECT count(distinct PULocationID) FROM `master-antenna-414015.green_taxi_nyc.taxi_rides` ```

### Q3 query
```SELECT count(1) FROM `master-antenna-414015.green_taxi_nyc.taxi_rides_int`  where fare_amount=0```

### Q4 query
```CREATE TABLE `master-antenna-414015.green_taxi_nyc.taxi_rides_partition`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT *
FROM `master-antenna-414015.green_taxi_nyc.taxi_rides_int`;```

### Q5  query
```SELECT distinct PULocationID FROM `master-antenna-414015.green_taxi_nyc.taxi_rides_partition` WHERE Date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30'```
