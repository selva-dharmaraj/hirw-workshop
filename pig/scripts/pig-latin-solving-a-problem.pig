/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Pig - Solving a Problem
/**************Hadoop In Real World**************/

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

### FILTERING ONLY RECORDS FROM YEAR 2003 ###

grunt> filter_by_yr = FILTER stocks by GetYear(date) == 2003;

### GROUPING RECORDS BY SYMBOL ###

grunt> grp_by_sym = GROUP filter_by_yr BY symbol;

grp_by_sym: {
	group: chararray,
	filter_by_yr: {
		(exchange: chararray,symbol: chararray,date: datetime,open: float,high: float,low: float,close: float,volume: int,adj_close: float)
	}
}

### SAMPLE OUTPUT OF GROUP ###

(CASC, { (NYSE,CASC,2003-12-22T00:00:00.000Z,22.02,22.2,21.94,22.09,36700,20.29), (NYSE,CASC,2003-12-23T00:00:00.000Z,22.15,22.15,21.9,22.05,23600,20.26), ....... })
(CATO, { (NYSE,CATO,2003-10-08T00:00:00.000Z,22.48,22.5,22.01,22.06,92000,12.0), (NYSE,CATO,2003-10-09T00:00:00.000Z,21.3,21.59,21.16,21.45,373500,11.67), ....... })

### CALCULATE AVERAGE VOLUME ON THE GROUPED RECORDS ###

avg_volume = FOREACH grp_by_sym GENERATE group, ROUND(AVG(filter_by_yr.volume)) as avgvolume;

### ORDER THE RESULT IN DESCENDING ORDER ###

avg_vol_ordered = ORDER avg_volume BY avgvolume DESC;

### STORE TOP 10 RECORDS ###

top10 = LIMIT avg_vol_ordered 10;
STORE top10 INTO 'output/pig/avg-volume' USING PigStorage(',');

### EXECUTE PIG INSTRUCTIONS AS SCRIPT ###

pig /hirw-workshop/pig/scripts/average-volume.pig

### PASSING PARAMETERS TO SCRIPT ###

pig -param input=/user/hirw/input/stocks -param output=output/pig/avg-volume-params /hirw-workshop/pig/scripts/average-volume-parameters.pig

### RUNNING A PIG SCRIPT LOCALLY. INPUT AND OUTPUT LOCATION ARE POINTING TO LOCAL FILE SYSTEM ###

pig -x local -param input=/hirw-workshop/input/stocks-dataset/stocks -param output=output/stocks /hirw-workshop/pig/scripts/average-volume-parameters.pig