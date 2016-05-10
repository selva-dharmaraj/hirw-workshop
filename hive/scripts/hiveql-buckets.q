/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive - Buckets
/**************Hadoop In Real World**************/

hive> DESCRIBE FORMATTED stocks_dynamic_partition;

hive> SHOW PARTITIONS stocks_dynamic_partition;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2003;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2003/sym=BUB;

### CREATE TABLE WITH 5 BUCKETS ###

hive> CREATE TABLE IF NOT EXISTS stocks_bucket (
exch STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
PARTITIONED BY (exch_name STRING, yr STRING)
CLUSTERED BY (symbol) INTO 5 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

hive> DESCRIBE FORMATTED stocks_bucket;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_bucket;

--Enable dynamic partition
hive> SET hive.exec.dynamic.partition=true;

--Set number of partitions per node
hive> SET hive.exec.max.dynamic.partitions=15000;
hive> SET hive.exec.max.dynamic.partitions.pernode=15000;

### ENFORCE BUCKETING ###

hive> SET hive.enforce.bucketing = true;

### INSERTING INTO A BUCKETTED TABLE ###

--This insert will create partitions (exch_name, yr) & buckets (symbol)
hive> INSERT OVERWRITE TABLE stocks_bucket
PARTITION (exch_name='ABCSE', yr)
SELECT *, year(ymd)
FROM stocks WHERE year(ymd) IN ('2001', '2002', '2003') and symbol like 'B%';

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_bucket/exch_name=ABCSE;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_bucket/exch_name=ABCSE/yr=2002;

hive> !hadoop fs -cat /user/hive/warehouse/stocks_db.db/stocks_bucket/exch_name=ABCSE/yr=2002/000000_0;


### TABLE SAMPLING ###

--Table sampling with out buckets
hive> SELECT *
FROM stocks TABLESAMPLE(BUCKET 3 OUT OF 5 ON symbol) s;

--Table sampling with buckets
hive> SELECT *
FROM stocks_bucket TABLESAMPLE(BUCKET 3 OUT OF 5 ON symbol) s;

