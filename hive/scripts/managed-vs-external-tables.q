/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive - Managed vs. External Tables
/**************Hadoop In Real World**************/

### MANAGED TABLE ###

hive> CREATE TABLE IF NOT EXISTS stocks (
exch string,
symbol string,
ymd string,
price_open float,
price_high float,
price_low float,
price_close float,
volume int,
price_adj_close float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


### DESCRIBE TO CHECK OUT THE TABLE TYPE ###

hive> DESCRIBE FORMATTED stocks_db.stocks;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks;


### DEMONSTRATE DATA LOSS WITH MANAGED TABLE ###

hive> SELECT * FROM stocks;

hive> DROP TABLE stocks;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks;

hive> SELECT * FROM stocks;


### EXTERNAL TABLE ###

hive> CREATE EXTERNAL TABLE IF NOT EXISTS stocks_ext (
exch string,
symbol string,
ymd string,
price_open float,
price_high float,
price_low float,
price_close float,
volume int,
price_adj_close float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


### DESCRIBE TO CHECK OUT THE TABLE TYPE ###

hive> DESCRIBE FORMATTED stocks_db.stocks_ext;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ext;

hadoop fs -copyFromLocal /hirw-workshop/input/stocks-dataset/stocks/* input/hive/stocks_db

hive> !hadoop fs -ls input/hive/stocks_db;


### LOAD EXTERNAL TABLE ###

hive> LOAD DATA INPATH 'input/hive/stocks_db'
INTO TABLE stocks_ext;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ext;

hive> SELECT * FROM stocks_ext;


### DROPPING TABLE WITH EXTERNAL TABLE - MAKE SURE DATA IS NOT LOST ###

hive> DROP TABLE stocks_ext;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ext;

hive> SELECT * FROM stocks_ext;


### LOCATION ATTRIBUTE ###

hadoop fs -copyFromLocal /hirw-workshop/input/stocks-dataset/stocks/* input/hive/stocks_db

hive> !hadoop fs -ls input/hive/stocks_db;

hive> CREATE EXTERNAL TABLE IF NOT EXISTS stocks_loc (
exch STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hirw/input/hive/stocks_db'
TBLPROPERTIES ('creator'='hirw', 'created_on' = '2015-02-16', 'description'='This table holds stocks data!!!');

hive> DESCRIBE FORMATTED stocks_db.stocks_loc;