-- Databricks notebook source
drop table if exists demo_db1.fire_service_calls_tbl;
drop view if exists demo_db1;

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/demo_db1.db

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo_db1

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo_db1.fire_service_calls_tbl(
CallNumber integer,
UnitID string, 
IncidentNumber integer, 
CallType string, 
CallDate string, 
WatchDate string, 
CallFinalDisposition string, 
AvailableDtTm string, 
Address string, 
City string, 
Zipcode integer, 
Battalion string,
StationArea string,
Box string, 
OriginalPriority string, 
Priority string, 
FinalPriority integer, 
ALSUnit boolean, 
CallTypeGroup string,
NumAlarms integer, 
UnitType string, 
UnitSequenceInCallDispatch integer, 
FirePreventionDistrict string, 
SupervisorDistrict string, 
Neighborhood string, 
Location string,
RowID string,
Delay float
) USING parquet

-- COMMAND ----------

insert into demo_db1.fire_service_calls_tbl values(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)

-- COMMAND ----------

truncate table demo_db1.fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fire_df = spark.read\
-- MAGIC     .format("csv")\
-- MAGIC         .option("header","true")\
-- MAGIC             .option("inferSchema","true")\
-- MAGIC                 .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv") 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fire_df.createGlobalTempView("fireview") #converts dataframe to table view

-- COMMAND ----------

-- we do not use insert command in big data world either data is loaded from files or from another tables
insert into demo_db1.fire_service_calls_tbl
select * from global_temp.fireview

-- COMMAND ----------

select * from demo_db1.fire_service_calls_tbl  
