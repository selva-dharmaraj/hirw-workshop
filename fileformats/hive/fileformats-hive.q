/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
File Formats - Hive
/**************Hadoop In Real World**************/

### ENABLE COMPRESSION ###

--Enable intermediate and output compression
hive> SET hive.exec.compress.intermediate=true;
hive> SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GZipCodec;
hive> SET hive.exec.compress.output=true;
hive> SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

hive> CREATE TABLE stocks_comp_on_gz
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
AS SELECT * FROM stocks;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_comp_on_gz;
 
hadoop fs -copyToLocal /user/hive/warehouse/stocks_db.db/stocks_comp_on_gz/000000_0.gz .

vi 000000_0.gz

hive> SELECT * FROM stocks_comp_on_gz;

### WORKING WITH SEQUENCE FILES ###

--Table for sequence files
hive> CREATE TABLE IF NOT EXISTS stocks_seq (
exch STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
STORED AS SEQUENCEFILE;

hive> DESCRIBE FORMATTED stocks_seq;

--Enable block level compression
hive> SET mapred.output.compression.type=BLOCK;

--Insert in to stocks_seq from stocks table. This will result in files under stocks_seq stored in Sequence file format.
hive> INSERT OVERWRITE TABLE stocks_seq
SELECT * FROM stocks;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_seq;

hadoop fs -copyToLocal /user/hive/warehouse/stocks_db.db/stocks_seq/000000_0 .

vi 000000_0

hive> SELECT * FROM stocks_seq
LIMIT 10;


### WORKING WITH AVRO FILES ###

hive> !hadoop fs -cat avro/stocks.avro.schema;
 
hive> CREATE TABLE IF NOT EXISTS stocks_avro
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/hirw/avro/stocks.avro.schema');

--Insert in to stocks_avro from stocks table. This will result in files under stocks_avro stored in Avro format.
hive> INSERT OVERWRITE TABLE stocks_avro
SELECT * FROM stocks;

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_avro;

rm 000000_0

hadoop fs -copyToLocal /user/hive/warehouse/stocks_db.db/stocks_avro/000000_0 .

vi 000000_0

hive> SELECT * FROM stocks_avro
LIMIT 10;