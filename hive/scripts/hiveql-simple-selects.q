/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive - HiveQL Simple Selects
/**************Hadoop In Real World**************/

### STANDARD SELECTS ###

hive> SELECT * FROM stocks
WHERE symbol = 'GEL';

hive> SELECT * FROM stocks
WHERE symbol IN ('GEL', 'B3B');

hive> SELECT * FROM stocks
WHERE exch LIKE 'ABC%' and symbol RLIKE 'B.B';

### SELECT WITH CASE STATEMENT ###

hive> SELECT symbol, price_open, price_close, volume,
CASE
 WHEN volume < 20000 THEN 'low'
 WHEN volume >= 20000 AND volume < 40000 THEN 'middle'
 WHEN volume >= 40000 AND volume < 60000 THEN 'high'
 ELSE 'very high'
END AS volume_level 
FROM stocks
WHERE symbol = 'GEL';

### DISTINCT & LIMIT ###

hive> SELECT DISTINCT exch, symbol FROM stocks;

hive> SELECT * FROM stocks
LIMIT 10;

### GROUP BY ###

hive> SELECT year(ymd), symbol, avg(volume) FROM stocks
GROUP BY year(ymd), symbol;

### GROUP BY & HAVING ###

hive> SELECT year(ymd), symbol, avg(volume) FROM stocks
GROUP BY year(ymd), symbol
HAVING avg(volume) > 400000;

hive> INSERT OVERWRITE LOCAL DIRECTORY '/home/hirw/output/hive/stocks'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT year(ymd), symbol, avg(volume) FROM stocks
GROUP BY year(ymd), symbol
HAVING avg(volume) > 400000;
