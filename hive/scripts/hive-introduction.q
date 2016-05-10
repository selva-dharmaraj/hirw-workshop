### www.hadoopinrealworld ###
### Hive Introduction - Create database and table ###


hive> CREATE DATABASE stocks_db;

hive> USE stocks_db;

hive> CREATE EXTERNAL TABLE IF NOT EXISTS stocks_tb (
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
LOCATION '/user/hirw/input/stocks';

hive> SELECT * FROM stocks_tb
LIMIT 100;
