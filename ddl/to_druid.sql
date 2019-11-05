SET hive.druid.indexer.partition.size.max=1000000;
SET hive.druid.indexer.memory.rownum.max=100000;
SET hive.tez.container.size=2048;
SET hive.druid.passiveWaitTimeMs=180000;

USE ny_taxi;
CREATE EXTERNAL TABLE trips_druid
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ( "druid.segment.granularity" = "WEEK",
  "druid.query.granularity" = "NONE" )
AS
SELECT
  npickup_datetime as `__time`,
  yearmonth,
  cast(`year` as STRING) as `year`,
  cast(`month` as STRING) as `month`,
  cast( DayofMonth as STRING ) as DayofMonth,
  date_format(npickup_datetime,'EEEEE') as `dayOfWeek`,
  cast(weekofyear(npickup_datetime) as STRING) as `weekofyear`,
  cast(HOUR(npickup_datetime) as STRING) as `hour`,
  cast(MINUTE(npickup_datetime) as STRING) as `minute`,
  cast(SECOND(npickup_datetime) as STRING) as `second`,
  payment_type,
  fare_amount,
  surcharge,
  mta_tax,
  tip_amount,
  tolls_amount,
  total_amount,
  minute(dropoff_datetime-npickup_datetime)) as trip_time,
  cast(trips.pickup_longitude as STRING) as longitude,
  cast(trips.pickup_latitude as STRING) as latitude
FROM trips
WHERE npickup_datetime IS NOT NULL;

-- CREATE AGGREGATED TABLE
-- SO WE GOT TO HAVE NB_TRIPS IN IT TO DISPLAY TOTAL TRIPS
CREATE EXTERNAL TABLE trips_druid2
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ( "druid.segment.granularity" = "MONTH",
  "druid.query.granularity" = "MINUTE" )
AS
SELECT
  npickup_datetime as `__time`,
  yearmonth,
  cast(`year` as STRING) as `year`,
  cast(`month` as STRING) as `month`,
  cast( DayofMonth as STRING ) as DayofMonth,
  date_format(npickup_datetime,'EEEEE') as `dayOfWeek`,
  cast(weekofyear(npickup_datetime) as STRING) as `weekofyear`,
  cast(HOUR(npickup_datetime) as STRING) as `hour`,
  cast(MINUTE(npickup_datetime) as STRING) as `minute`,
  cast(SECOND(npickup_datetime) as STRING) as `second`,
  payment_type,
  sum(fare_amount) as fare_amount,
  sum(surcharge) as surcharge,
  sum(mta_tax) as mta_tax,
  sum(tip_amount) as tip_amount,
  sum(tolls_amount) as tolls_amount,
  sum(total_amount) as total_amount,
  sum(minute(dropoff_datetime-npickup_datetime)) as trip_time,
  sum(1) as nb_trips
FROM trips
WHERE npickup_datetime IS NOT NULL
GROUP BY npickup_datetime,yearmonth,year,month,DayofMonth,weekofyear,payment_type ;
