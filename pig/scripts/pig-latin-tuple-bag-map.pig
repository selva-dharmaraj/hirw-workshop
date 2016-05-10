/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Pig - Complex Types - Tuple, Bag & Map
/**************Hadoop In Real World**************/


grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

grunt> filter_by_yr = FILTER stocks by GetYear(date) == 2003;

### STRUCTURE OF A GROUP BY RESULTSET ###

grunt> grp_by_sym = GROUP filter_by_yr BY symbol;

grp_by_sym: {
	group: chararray,
	filter_by_yr: {
		(exchange: chararray,symbol: chararray,date: datetime,open: float,high: float,low: float,close: float,volume: int,adj_close: float)
	}
}

(CASC, { (ABCSE,CASC,2003-12-22T00:00:00.000Z,22.02,22.2,21.94,22.09,36700,20.29), (ABCSE,CASC,2003-12-23T00:00:00.000Z,22.15,22.15,21.9,22.05,23600,20.26), ....... })
(CATO, { (ABCSE,CATO,2003-10-08T00:00:00.000Z,22.48,22.5,22.01,22.06,92000,12.0), (ABCSE,CATO,2003-10-09T00:00:00.000Z,21.3,21.59,21.16,21.45,373500,11.67), ....... })


### PROJECTING NESTED COLUMNS ###

close_by_sym = FOREACH grp_by_sym GENERATE group, filter_by_yr.close;

(CASC,{(19.5),(15.76),(15.73),(15.75),(15.8),(15.55),(15.85),(15.9),(15.85),(15.94),(16.16),(16.06),(15.84),(15.79),(15.75),........})
(CATO,{(22.06),(21.45),(20.9),(21.33),(21.0),(20.94),(21.05),(20.98),(20.68),(20.69),(20.14),(20.1),(19.95),(20.64),(20.94),........})

### FINDING MAX CLOSE PRICE BY SYMBOL ###

max_close_by_sym = FOREACH grp_by_sym GENERATE group, MAX(filter_by_yr.close);

(BBVA,13.85)
(BRFS,17.35)
(CACI,52.03)
(CASC,27.39)
(CATO,25.11)

### GROUPING BY MORE THAN ONE COLUMN ###

grunt> grp_by_sym_yr = GROUP stocks BY (symbol, GetYear(date));


grunt> DESCRIBE grp_by_sym_yr;
grp_by_sym_yr: {
	group: (symbol: chararray, org.apache.pig.builtin.getyear_date_123: int),
	stocks: {
		(exchange: chararray,symbol: chararray,date: datetime,open: float,high: float,low: float,close: float,volume: int,adj_close: float)
	}
}


grunt> max_close_by_sym_yr = FOREACH grp_by_sym_yr GENERATE group, MAX(stocks.close);

((CATO,2001),21.75)
((CATO,2002),27.44)
((CATO,2003),25.11)
((CATO,2004),29.44)
((CATO,2005),33.26)
((CATO,2006),26.25)
((CATO,2007),25.01)
((CATO,2008),19.38)
((CATO,2009),22.86)
((CATO,2010),21.84)
((CHSP,2010),19.25)
((CLNY,2009),20.75)
((CLNY,2010),20.99)
((DEXO,2010),34.4)
((DOLE,2009),12.5)
((DOLE,2010),12.44)

### PROJECT INDIVIDUAL COLUMNS FROM GROUP ###

grunt> max_close_by_sym_yr = FOREACH grp_by_sym_yr GENERATE group.symbol, group.$1, MAX(stocks.close);

DUMP max_close_by_sym_yr;

### STRUCTURE OF GROUP RESULT BY TWO COLUMNS ###

--If you like the columns to be delimited by comma - by default delimiter is comma
grunt> STORE grp_by_sym_yr INTO 'output/pig/grp_by_sym_yr';

--If you like the columns to be delimited by comma - by default delimiter is comma
grunt> STORE grp_by_sym_yr INTO 'output/pig/grp_by_sym_yr' USING PigStorage(',') ;

(DOLE,2010)     {(ABCSE,DOLE,2010-02-05T00:00:00.000Z,11.1,11.14,10.75,10.99,827400,10.99),(ABCSE,DOLE,2010-02-04T00:00:00.000Z,11.38,11.43,11.02,11.15,510000,11.15),(ABCSE,DOLE,2010-02-03T00:00:00.000Z,11.26,11.58,11.26,11.34,337600,11.34),(ABCSE,DOLE,2010-02-02T00:00:00.000Z,11.77,11.86,11.33,11.35,737600,11.35),(ABCSE,DOLE,2010-02-01T00:00:00.000Z,11.59,11.88,11.48,11.86,463400,11.86),(ABCSE,DOLE,2010-01-29T00:00:00.000Z,11.61,11.65,11.41,11.5,472000,11.5),(ABCSE,DOLE,2010-01-28T00:00:00.000Z,11.94,11.94,11.51,11.62,396700,11.62),(ABCSE,DOLE,2010-01-27T00:00:00.000Z,11.82,11.94,11.66,11.88,312900,11.88),(ABCSE,DOLE,2010-01-26T00:00:00.000Z,11.65,12.02,11.63,11.84,400200,11.84),(ABCSE,DOLE,2010-01-25T00:00:00.000Z,11.83,11.88,11.61,11.74,331400,11.74),(ABCSE,DOLE,2010-01-22T00:00:00.000Z,12.13,12.13,11.7,11.8,416200,11.8),(ABCSE,DOLE,2010-01-21T00:00:00.000Z,12.43,12.43,12.01,12.1,511800,12.1),(ABCSE,DOLE,2010-01-20T00:00:00.000Z,12.32,12.45,12.2,12.44,487700,12.44),(ABCSE,DOLE,2010-01-19T00:00:00.000Z,12.25,12.33,12.19,12.33,262800,12.33),(ABCSE,DOLE,2010-01-15T00:00:00.000Z,12.41,12.42,12.11,12.2,228600,12.2),(ABCSE,DOLE,2010-01-14T00:00:00.000Z,12.25,12.41,12.2,12.41,1442800,12.41),(ABCSE,DOLE,2010-01-13T00:00:00.000Z,12.15,12.32,12.08,12.28,279700,12.28),(ABCSE,DOLE,2010-01-12T00:00:00.000Z,12.04,12.23,12.01,12.16,267800,12.16),(ABCSE,DOLE,2010-01-11T00:00:00.000Z,12.1,12.15,11.92,12.11,725800,12.11),(ABCSE,DOLE,2010-01-08T00:00:00.000Z,12.2,12.29,11.78,12.0,964100,12.0),(ABCSE,DOLE,2010-01-07T00:00:00.000Z,12.26,12.27,12.01,12.25,801200,12.25),(ABCSE,DOLE,2010-01-06T00:00:00.000Z,12.14,12.21,12.01,12.21,640800,12.21),(ABCSE,DOLE,2010-01-05T00:00:00.000Z,12.06,12.24,12.03,12.13,280800,12.13),(ABCSE,DOLE,2010-01-04T00:00:00.000Z,12.35,12.45,12.02,12.04,269400,12.04),(ABCSE,DOLE,2010-02-08T00:00:00.000Z,11.01,11.01,10.81,10.85,396700,10.85)}

### DEFINING A PIG RELATION USING COMPLEX TYPES ###

grunt> complex_type = LOAD 'output/pig/grp_by_sym_yr' as  (grp:tuple(sym:chararray, yr:int), stocks_b:{stocks_t:(exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float, volume:int, adj_close:float)}) ;


grunt> max_vol_by_yr = FOREACH complex_type GENERATE grp.sym, grp.yr, MAX(stocks_b.volume);
grunt> limit10 = LIMIT max_vol_by_yr 10;
grunt> DUMP limit10;

(B3,1990,456000)
(B3,1991,462000)
(B3,1992,579000)
(B3,1993,1003600)
(B3,1994,1294000)
(B3,1995,551200)
(B3,1996,882800)
(B3,1997,632400)
(B3,1998,540000)
(B3,1999,1054000)


### COMPLEX TYPE - MAP ###

328;ADMIN HEARNG;[street#939 W El Camino,city#Chicago,state#IL]
43;ANIMAL CONTRL;[street#415 N Mary Ave,city#Chicago,state#IL]

grunt> departments = LOAD '/user/hirw/input/employee-pig/department_dataset_chicago' using PigStorage(';') AS (dept_id:int, dept_name:chararray, address:map[]);

grunt> dept_addr = FOREACH departments GENERATE dept_name, address#'street' as street, address#'city' as city, address#'state' as state;
