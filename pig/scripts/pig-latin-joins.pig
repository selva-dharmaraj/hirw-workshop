/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Pig Latin - Joins
/**************Hadoop In Real World**************/

### LOAD stocks ###

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### LOAD dividends ###

grunt> divs = LOAD '/user/hirw/input/dividends' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, dividends:float);
grunt> DESCRIBE divs;
divs: 
	{
		exchange: chararray,
		symbol: chararray,
		date: datetime,
		dividends: float
	}

### LOAD companies ###	

companies = LOAD '/user/hirw/input/companies' USING PigStorage(';') as (symbol:chararray, name:chararray, address: map[]);

cmp = FOREACH companies GENERATE symbol, name, address#'street', address#'city', address#'state';


### INNER JOIN ###

grunt> join_inner = JOIN stocks BY (symbol, date) , divs BY (symbol, date);	

grunt> DESCRIBE join_inner;

join_inner: {
	stocks::exchange: chararray,
	stocks::symbol: chararray,
	stocks::date: datetime,
	stocks::open: float,
	stocks::high: float,
	stocks::low: float,
	stocks::close: float,
	stocks::volume: int,
	stocks::adj_close: float,
	divs::exchange: chararray,
	divs::symbol: chararray,
	divs::date: datetime,
	divs::dividends: float
	}

grunt> join_project  = FOREACH join_inner GENERATE stocks::symbol, divs::date, divs::dividends;

grunt> DUMP join_project;

### LEFT OUTER ###

grunt> join_left = JOIN stocks BY (symbol, date) LEFT OUTER, divs BY (symbol, date);

grunt> DUMP join_left;

--Filter out records with no dividends
grunt> filterleftjoin = FILTER join_left BY divs::symbol IS NOT NULL;

grunt> DUMP filterleftjoin;


### RIGHT OUTER ###

grunt> join_right = JOIN stocks BY (symbol, date) RIGHT OUTER, divs BY (symbol, date);

grunt> DUMP join_right;


### FULL JOIN ###

--FULL OUTER will display rows from both sides matched and unmatched. Combination of LEFT OUTER and RIGHT OUTER
grunt> join_full = JOIN stocks BY (symbol, date) FULL, divs BY (symbol, date);

grunt> DUMP join_full;


### Multiway Join ###

grunt> join_multi = JOIN stocks by (symbol, date), divs by (symbol, date), cmp by symbol;

--Multiway join is only possible on inner joins and not on outer joins
grunt> join_multi = JOIN stocks by symbol, divs by symbol, cmp by symbol;

grunt> DUMP join_multi;

###CROSS###

--Takes every record in stocks and combines it with every record in divs
grunt> crs = CROSS stocks, divs;

--What about non equi joins?

grunt> non_equi = FILTER crs by stocks::symbol != divs::symbol;

grunt> limit1000 = LIMIT non_equi 1000; 


###COGROUP###

grunt> cgrp = COGROUP stocks BY (symbol, date), divs by (symbol, date);

cgrp: {
	group: (symbol: chararray,date: chararray),
	stocks: {(exchange: chararray,symbol: chararray,date: chararray,open: float,high: float,low: float,close: float,volume: int,adj_close: float)},
	divs: {(exchange: bytearray,symbol: bytearray,date: bytearray,dividends: bytearray)}
	}



((CSL,2009-05-14),{},{(ABCSE,CSL,2009-05-14,0.155)})
((CSL,2009-08-12),{(ABCSE,CSL,2009-08-12,32.65,32.73,32.17,32.54,528900,32.39)},{(ABCSE,CSL,2009-08-12,0.16)})
((CSL,2009-08-13),{(ABCSE,CSL,2009-08-13,32.58,33.19,32.49,33.15,447600,32.99)},{})

grunt> filter_empty_divs = FILTER cgrp BY (NOT IsEmpty(stocks)) AND  (NOT IsEmpty(divs));

grunt> limit10 = LIMIT filter_empty_divs 10;

grunt> DUMP limit10;


filter_empty_divs = FILTER cgrp BY (IsEmpty(group));
limit10 = LIMIT filter_empty_divs 10;
DUMP limit10;
