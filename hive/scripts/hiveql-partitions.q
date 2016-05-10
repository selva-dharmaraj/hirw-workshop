/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive - Partitions
/**************Hadoop In Real World**************/

--This query will traverse all the files to bring the result set
hive> SELECT * FROM stocks 
WHERE symbol = 'XYZ' and ymd = '2000-07-03';

### PARTITON TABLE ON SYMBOL COLUMN ###

--Table with one partition column - sym
hive> CREATE TABLE IF NOT EXISTS stocks_partition (
exch STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
PARTITIONED BY (sym STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

hive> DESCRIBE FORMATTED stocks_partition;

### LOADING PARTITIONS USING INSERT...SELECT ###

--Add and load a partition using INSERT
hive> INSERT OVERWRITE TABLE stocks_partition
PARTITION (sym = 'B7J')
SELECT * FROM stocks s
WHERE s.symbol = 'B7J';

hive> INSERT OVERWRITE TABLE stocks_partition
PARTITION (sym = 'BB3')
SELECT * FROM stocks s
WHERE s.symbol = 'BB3';

### DETAILS ABOUT PARTITON ###

--To see the list of partitions in a table
hive> SHOW PARTITIONS stocks_partition;

hive> DESCRIBE FORMATTED stocks_partition;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_partition;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_partition/sym=B7J;

--SELECT with partition column in the where clause
hive> SELECT * FROM stocks_partition
WHERE sym='B7J';

--Extract only CBZ records in to a directory
hive> INSERT OVERWRITE DIRECTORY 'output/hive/stocks-zuu'
SELECT *
FROM stocks WHERE symbol='ZUU';

### ADD PARTITION USING LOCATION ###

--Add a partition using a location
hive> ALTER TABLE stocks_partition ADD IF NOT EXISTS
PARTITION (sym = 'ZUU') LOCATION 'output/hive/stocks-zuu';

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_partition;

hive> SHOW PARTITIONS stocks_partition;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_partition;

### CREATE MULTIPLE PARTITIONS USING INSERT ###

--Multiple partitions using single insert
hive> FROM stocks s
INSERT OVERWRITE TABLE stocks_partition
PARTITION (sym = 'GEL')
SELECT * WHERE s.symbol = 'GEL'
INSERT OVERWRITE TABLE stocks_partition
PARTITION (sym = 'GEK')
SELECT * WHERE s.symbol = 'GEK';

hive> SHOW PARTITIONS stocks_partition;

### DROPPING A PARTITION ###

hive> ALTER TABLE stocks_partition DROP IF EXISTS PARTITION(sym = 'GEL');

hive> SHOW PARTITIONS stocks_partition;

### INCORRECT PARTITION ASSIGNMENT ###

hive> INSERT OVERWRITE TABLE stocks_partition
PARTITION (sym = 'APPL')
SELECT * FROM stocks s
WHERE s.symbol = 'MSFT';

### ENABLE DYNAMIC PARTITION ###

hive> SET hive.exec.dynamic.partition=true;

hive> INSERT OVERWRITE TABLE stocks_partition
PARTITION (sym)
SELECT s.*, s.symbol
FROM stocks s;

--Setting dynamic partition mode to nonstrict. Property is strict by default
hive> SET hive.exec.dynamic.partition.mode=nonstrict;

### CREATE TABLE WITH MORE THAN ONE PARTITION COLUMNS ###

--Table with 3 partition columns - exch_name, yr, sym
hive> CREATE TABLE IF NOT EXISTS stocks_dynamic_partition (
exch STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
PARTITIONED BY (exch_name STRING, yr STRING, sym STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

hive> DESCRIBE FORMATTED stocks_dynamic_partition;

### DYNAMIC PARTITION INSERTS ###

hive> INSERT OVERWRITE TABLE stocks_dynamic_partition
PARTITION (exch_name='ABCSE', yr, sym)
SELECT *, year(ymd), symbol
FROM stocks;

hive> INSERT OVERWRITE TABLE stocks_dynamic_partition
PARTITION (exch_name='ABCSE', yr, sym)
SELECT *, year(ymd), symbol
FROM stocks WHERE year(ymd) IN ('2001', '2002', '2003') and symbol like 'B%';

--Set the number of partitions per node. 
hive> SET hive.exec.max.dynamic.partitions=1000;
hive> SET hive.exec.max.dynamic.partitions.pernode=500;

hive> SHOW PARTITIONS stocks_dynamic_partition;

### TRAVERSE PARTITIONS IN HDFS ###

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_dynamic_partition;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_dynamic_partition/exch_name=ABCSE;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2003;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2003/sym=GEL;

hive> !hadoop fs -cat /user/hive/warehouse/stocks_db.db/stocks_dynamic_partition/exch_name=ABCSE/yr=2003/sym=GEL/000000_0;

### SELECTING TARGETTED PARTIITONS ###

hive> SELECT * FROM stocks_dynamic_partition 
WHERE yr=2003 and volume > 10000;

hive> SELECT * FROM stocks_dynamic_partition
WHERE yr=2003 and sym='GEL'  and volume > 10000;

--Property to control Hive's mode
hive> SET hive.mapred.mode=strict;

hive> SELECT * FROM stocks_dynamic_partition
WHERE volume > 10000;

hive> SELECT * FROM stocks_dynamic_partition
WHERE exch_name = 'ABCSE' and volume > 10000;