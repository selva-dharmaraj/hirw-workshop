/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive QL - Joins
/**************Hadoop In Real World**************/


### CREATING DIVIDENDS DATASET ###

hive> CREATE EXTERNAL TABLE IF NOT EXISTS dividends (
exch STRING,
symbol STRING,
ymd STRING,
dividend FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hirw/input/dividends';

hive> SELECT * FROM dividends
LIMIT 100;

### INNER JOIN ###

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s INNER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;

### LEFT OUTER JOIN ###

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s LEFT OUTER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s LEFT OUTER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd
WHERE d.dividend IS NOT NULL;

### RIGHT OUTER JOIN ###

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s RIGHT OUTER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;

### FULL OUTER JOIN ###

hive> SELECT s.exch, s.symbol, s.ymd, s.price_close, d.dividend
FROM stocks s FULL OUTER JOIN dividends d 
ON s.symbol = d.symbol AND s.ymd = d.ymd;

### LEFT SEMI JOIN ###

--Implements IN/EXISTS. As of  As of Hive 0.13 the IN/NOT IN/EXISTS/NOT EXISTS operators are supported using subqueries so most of these JOINs don't have to be performed manually anymore.
hive> SELECT s.ymd, s.symbol, s.price_close
FROM stocks s LEFT SEMI JOIN dividends d 
ON s.ymd = d.ymd AND s.symbol = d.symbol;

### INEQUALITY JOIN ###

--Join with inequality condition
hive> SELECT s.ymd, s.symbol, s.price_close
FROM stocks s LEFT SEMI JOIN dividends d 
ON s.ymd > d.ymd;

--CROSS JOIN (for inequality joins)
hive> SELECT s.ymd, s.symbol, s.price_close
FROM stocks s CROSS JOIN dividends d 
WHERE s.ymd > d.ymd;

### MUTLITABLE JOINS & NUMBER OF MR JOBS ###

--Same key. Only one MR job
SELECT a.val, b.val, c.val FROM a JOIN b ON (a.key = b.key1) JOIN c ON (c.key = b.key1)

--2 MR Job. The first map/reduce job joins a with b and the results are then joined with c in the second map/reduce job.
SELECT a.val, b.val, c.val FROM a JOIN b ON (a.key = b.key1) JOIN c ON (c.key = b.key2)