/**************Hadoop In Real World**************/
Hadoop In Real World *** http://www.hadoopinrealworld.com
Hive Collection Data Types - Twitter Dataset
/**************Hadoop In Real World**************/

### PROGRAM TO DOWNLOAD TWEETS FROM LIVE TWITTER STREAM ###

java -jar /hirw-workshop/hive/twitter/twitter.jar twitter.properties twitter.json

### CREATE TWITTER DATABASE ###

hive> CREATE DATABASE twitter;

hive> USE twitter;

### TABLE FOR TWITTER DATASET ###

--Add jar to include JSONSerDe in create table
hive> ADD JAR /hirw-workshop/hive/lib/hive-serdes-1.0-SNAPSHOT.jar;

--Create external table to point to twitter dataset.
hive> CREATE EXTERNAL TABLE IF NOT EXISTS tweets (
  text STRING,
  entities STRUCT<
    hashtags:ARRAY<STRUCT<text:STRING>>>,
  user STRUCT<
    screen_name:STRING,
    friends_count:INT,
    followers_count:INT,
    statuses_count:INT,
    verified:BOOLEAN,
    utc_offset:INT,
    time_zone:STRING>
) 
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/hirw/input/twitter';


### INFLUENTIAL TWITTER USERS BASED ON FOLLOWERS COUNT & CONVERSATIONS ###

hive> SELECT DISTINCT user.screen_name as name, user.followers_count as count
FROM tweets
WHERE size(entities.hashtags) > 0 
ORDER BY count DESC
LIMIT 5;