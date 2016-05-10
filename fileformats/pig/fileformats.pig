/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
File Formats - Pig
/**************Hadoop In Real World**************/


### ENABLE COMPRESSION ###

grunt> SET mapred.compress.map.output true;
grunt> SET mapred.output.compress true;
grunt> SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;


grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',')  as (exchange:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

### STORE stocks AS COMPRESSED FILE ###

grunt> STORE stocks INTO 'output/pig/compressed' USING PigStorage(',');

### VERIFY FILE IS COMPRESSED ###

hadoop fs -ls output/pig/compressed

hadoop fs -copyToLocal output/pig/compressed/part-m-00000.gz

tail part-m-00000.gz

### LOAD A COMPRESSED FILE ###

grunt> stocks_comp = LOAD 'output/pig/compressed' USING PigStorage(',')  as (exchange:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

grunt> top10 = LIMIT stocks_comp 10;
grunt> DUMP top10;


### LOADING A SEQUENCE FILE ###

grunt> REGISTER /usr/lib/pig/piggybank.jar;
grunt> DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
grunt> seq_dataset = LOAD '/user/hirw/output/fileformats/sequence-file/compressed-sequence-file' USING SequenceFileLoader AS (key:long, value:chararray);

grunt> split_value = FOREACH seq_dataset GENERATE FLATTEN(STRSPLIT(value, ',', 9)); 

grunt> sym_vol = FOREACH split_value GENERATE (chararray)$1 as symbol, (double)$7 as volume;
grunt> grp_sym = GROUP sym_vol BY symbol;
grunt> avg_vol = FOREACH grp_sym GENERATE group, AVG(sym_vol.volume);

grunt> top10 = LIMIT avg_vol 10;
grunt> DUMP top10;

### STORING A SEQUENCE FILE ###

REGISTER '/hirw-workshop/elephant-bird/lib/elephant-bird-pig-4.0.jar';
REGISTER '/hirw-workshop/elephant-bird/lib/elephant-bird-hadoop-compat-4.0.jar';
REGISTER '/hirw-workshop/elephant-bird/lib/elephant-bird-core-4.0.jar';

STORE avg_vol INTO 'output/pig/fileformats/sequence-file/pig-sequence' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
  '-c com.twitter.elephantbird.pig.util.TextConverter', 
  '-c com.twitter.elephantbird.pig.util.TextConverter'
);


### VERIFY FILE IS A SEQUENCE FILE ###

hadoop fs -ls output/pig/fileformats/sequence-file/pig-sequence

hadoop fs -copyToLocal output/pig/fileformats/sequence-file/pig-sequence/part-r-00000

vi part-r-00000

hadoop fs -text output/pig/fileformats/sequence-file/pig-sequence/part-r-00000

### STORE AN AVRO FILE ###

REGISTER /usr/lib/pig/piggybank.jar
REGISTER /usr/lib/pig/lib/avro-*.jar
REGISTER /usr/lib/pig/lib/jackson-core-asl-*.jar
REGISTER /usr/lib/pig/lib/jackson-mapper-asl-*.jar
REGISTER /usr/lib/pig/lib/json-simple-*.jar

grunt> stocks = LOAD '/user/hirw/input/stocks' USING PigStorage(',')  as (exchange:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

grunt> STORE stocks INTO 'output/pig/avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
        '{
            "schema": {
				"namespace": "com.hirw.avro",
				 "type": "record",
				 "name": "Stock",
				 "fields": [
					 {"name": "exch", "type": "string"},
					 {"name": "symbol",  "type": ["string", "null"]},
					 {"name": "ymd", "type": ["string", "null"]},
					 {"name": "price_open", "type": "float"},
					 {"name": "price_high",  "type": ["float", "null"]},
					 {"name": "price_low", "type": ["float", "null"]},
					 {"name": "price_close", "type": "float"},
					 {"name": "volume",  "type": ["int", "null"]},
					 {"name": "price_adj_close", "type": ["float", "null"]}
				 ]
            }
        }');
		
### VERIFY FILE IS AN AVRO FILE ###

hadoop fs -ls output/pig/avro
rm part-m-00000.avro
hadoop fs -copyToLocal output/pig/avro/part-m-00000.avro
vi part-m-00000.avro

### LOAD AN AVRO FILE ###

grunt> stocks_avro = LOAD 'output/pig/avro'
          USING org.apache.pig.piggybank.storage.avro.AvroStorage(
		  'no_schema_check',
          'schema_file', 
		  'avro/stocks.avro.schema');
		  
grunt> top10 = LIMIT stocks_avro 10;
grunt> DUMP top10;		  