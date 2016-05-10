--Load with column names and datatypes
prices = LOAD '/user/hirw/input/stocks/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,volume:int, adj_close:float);

--Filter only records for 2003
filter_by_yr = FILTER prices by GetYear(date) == 2003;

--Group records by symbol
grp_by_sym = GROUP filter_by_yr BY symbol;

--Calculate average volume on the grouped records
avg_volume = FOREACH grp_by_sym GENERATE group, ROUND(AVG(filter_by_yr.volume)) as avgvolume;

--Order the result in descending order 
avg_vol_ordered = ORDER avg_volume BY avgvolume DESC;

--Only top 10 records
top10 = LIMIT avg_vol_ordered 10;

--Store result in to HDFS
STORE top10 INTO 'output/pig/avg-volume' USING PigStorage(',');