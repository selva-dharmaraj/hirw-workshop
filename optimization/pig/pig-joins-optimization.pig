/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Optimization - Pig Joins
/**************Hadoop In Real World**************/

### LOAD stocks ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### LOAD dividends ###

grunt> divs = LOAD '/user/hirw/input/dividends' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:chararray, dividends:float);


### JOIN ###

join_symbol = JOIN divs BY (symbol,date), stocks BY (symbol,date);

### FRAGMENT REPLICATE JOIN ###

join_replicated = JOIN stocks BY (symbol,date), divs BY (symbol,date) USING 'REPLICATED';

### SKEW JOIN ###

join_skewed = JOIN divs BY (symbol,date), stocks BY (symbol,date) USING 'SKEWED';

### SORT MERGE JOIN ###

join_merge = JOIN stocks BY (symbol,date), divs BY (symbol,date) USING 'MERGE';