/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive - Order By vs. Sort By vs. Cluster By
/**************Hadoop In Real World**************/

hive> SELECT * FROM stocks
ORDER BY price_close DESC;

hive> SET mapreduce.job.reduces=3;

hive> SELECT * FROM stocks
ORDER BY price_close DESC;

hive> SELECT ymd, symbol, price_close
FROM stocks WHERE year(ymd) = '2003'
SORT BY symbol ASC, price_close DESC;

hive> INSERT OVERWRITE LOCAL DIRECTORY '/home/hirw/output/hive/stocks'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT ymd, symbol, price_close
FROM stocks WHERE year(ymd) = '2003'
SORT BY symbol ASC, price_close DESC;

hive> INSERT OVERWRITE LOCAL DIRECTORY '/home/hirw/output/hive/stocks'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT ymd, symbol, price_close
FROM stocks WHERE year(ymd) = '2003'
DISTRIBUTE BY symbol
SORT BY symbol ASC, price_close DESC;

hive> SELECT ymd, symbol, price_close
FROM stocks
DISTRIBUTE BY symbol
SORT BY symbol ASC;

hive> INSERT OVERWRITE LOCAL DIRECTORY '/home/hirw/output/hive/stocks'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT ymd, symbol, price_close
FROM stocks
CLUSTER BY symbol;