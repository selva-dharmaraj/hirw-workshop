/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive - Loading Hive Tables
/**************Hadoop In Real World**************/

### CREATE A TABLE FOR STOCKS ###

hive> CREATE TABLE IF NOT EXISTS stocks (
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
TBLPROPERTIES ('creator'='hirw', 'created_on' = '2015-02-16', 'description'='This table holds stocks data!!!');

### DESCRIBE TABLE TO GET DETAILS ABOUT TABLE ###

hive> DESCRIBE FORMATTED stocks;

### COPY THE STOCKS DATASET TO HDFS ###

hadoop fs -copyFromLocal /hirw-workshop/input/stocks-dataset/stocks/* input/hive/stocks_db

hadoop fs -ls input/hive/stocks_db

hive> !hadoop fs -ls input/hive/stocks_db;

### LOAD DATASAET USING LOAD COMMAND ###

hive> LOAD DATA INPATH 'input/hive/stocks_db'
INTO TABLE stocks;

hive> !hadoop fs -ls input/hive/stocks_db;

hive> DESCRIBE FORMATTED stocks;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks;

hive> SELECT * FROM stocks;

### LOAD DATASET USING CTAS ###

hive> CREATE TABLE stocks_ctas
AS
SELECT * FROM stocks;

hive> DESCRIBE FORMATTED stocks_ctas;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ctas;

### LOAD DATASET USING INSERT..SELECT ###

hive> INSERT INTO TABLE stocks_ctas
SELECT s.* FROM stocks s;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ctas;

hive> SELECT * FROM stocks_ctas;

### LOAD DATASET USING INSERT OVERWRITE ###

hive> INSERT OVERWRITE TABLE stocks_ctas
SELECT s.* FROM stocks s;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ctas;

hadoop fs -copyFromLocal /home/cloudera/hirw-workshop/input/stocks_db/stocks/* input/stocks_db/stocks

hadoop fs -ls input/stocks_db/stocks

### LOCATION ATTRIBUTE & LOADING DATA ### 

hadoop fs -copyFromLocal /hirw-workshop/input/stocks-dataset/stocks/* input/hive/stocks_db

hadoop fs -ls input/hive/stocks_db

hive> CREATE TABLE IF NOT EXISTS stocks_loc (
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

hive> DESCRIBE FORMATTED stocks_loc;

hive> SELECT * FROM stocks_loc;