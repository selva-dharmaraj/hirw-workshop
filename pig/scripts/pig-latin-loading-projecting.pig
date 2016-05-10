/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Pig Latin - Loading & Projecting Datasets
/**************Hadoop In Real World**************/

### LOADING A DATASET ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### STRUCTURE ###

grunt> DESC stocks;

### PROJECT AND MANIPULATE FEW COLUMNS FROM DATASET ###

grunt> projection = FOREACH stocks GENERATE symbol, SUBSTRING($0, 0, 1) as sub_exch, close - open as up_or_down;

### PRINT RESULT ON SCREEN ###

grunt> DUMP projection;

### STORE RESULT IN HDFS ###

grunt> STORE projection INTO 'output/pig/simple-projection';

### LOAD 1 - WITH NO COLUMN NAMES AND DATATYPES ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',');

### LOAD 2 - WITH COLUMN NAMES BUT NO DATATYPES ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange, symbol, date, open, high, low, close, volume, adj_close);

### LOAD 3 - WITH COLUMN NAMES AND DATATYPES ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### TO LOOK UP STRUCTURE OF THE RELATION ###

grunt> DESCRIBE stocks;

### WHEN COLUMN NAMES ARE NOT AVAILABLE ###

grunt> projection = FOREACH stocks GENERATE $1 as symbol, SUBSTRING($0, 0, 1) as sub_exch, $6 - $3 as up_or_down;