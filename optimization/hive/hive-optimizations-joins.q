/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive Optimizations
/**************Hadoop In Real World**************/

hive> USE stocks_db;

### REGULAR INNER JOIN ###

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s INNER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;

### INNER JOIN WITH BETTER TABLE PLACEMENT FOR OPTIMIZATION ###

--Inner join positioning stocks table to the right
hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM dividends d INNER JOIN stocks s
ON s.symbol = d.symbol AND s.ymd = d.ymd;

### STREAM A TABLE TO REDUCER ###

hive> SELECT /*+ STREAMTABLE(s) */ s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s INNER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;


### ENABLE AUTO CONVERT JOIN ###

hive> SET hive.auto.convert.join = true;

### MAPJOIN HINT ###

--Records from Dividends will be loaded in memory and join will be performed entirely on Map side
hive> SELECT /*+ MAPJOIN(d) */ s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s INNER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s INNER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;


### JOINS WITH DATASET DERIVED AT RUN TIME ###

hive> SELECT * FROM dividends d
INNER JOIN (SELECT * FROM stocks WHERE volume > 100000) s
ON s.symbol = d.symbol AND s.ymd = d.ymd;

--Property to set the size for "small" files
hive> hive.mapjoin.smalltable.filesize=30000000;

--Enable bucketing
hive> SET hive.enforce.bucketing = true;

### SMB MAP JOIN ###

--Create stocks_smb table with 10 buckets. Bucket the table based on symbol column and also sort by symbol column.
hive> CREATE TABLE IF NOT EXISTS stocks_smb (
exch STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
CLUSTERED BY (symbol) SORTED BY (symbol) INTO 10 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

hive> INSERT OVERWRITE TABLE stocks_smb
SELECT * FROM stocks;

--Create dividends_smb table with 5 buckets. Bucket the table based on symbol column and also sort by symbol column.
hive> CREATE TABLE IF NOT EXISTS dividends_smb (
exch STRING,
symbol STRING,
ymd STRING,
dividend FLOAT
)
CLUSTERED BY (symbol) SORTED BY (symbol) INTO 5 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

hive> INSERT OVERWRITE TABLE dividends_smb
SELECT * FROM dividends;

### PROPERTIES TO ENABLE SMB MAP JOIN ###

hive> SET hive.auto.convert.sortmerge.join=true;
hive> SET hive.optimize.bucketmapjoin = true;
hive> SET hive.optimize.bucketmapjoin.sortedmerge = true;
hive> SET hive.auto.convert.sortmerge.join.noconditionaltask=true;

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks_smb s INNER JOIN dividends_smb d 
ON s.symbol = d.symbol;