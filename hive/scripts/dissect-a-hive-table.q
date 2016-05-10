### www.hadoopinrealworld.com ###
### Dissect a Hive Table ###

hive> CREATE DATABASE stocks_db;

hive> SHOW DATABASES;

hive> USE stocks_db;

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

hive> DESCRIBE FORMATTED stocks;

hive> DROP DATABASE stocks_db;

hive> DROP TABLE stocks;

hive> DROP DATABASE stocks_db CASCADE;

