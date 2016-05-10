### www.hadoopinrealworld.com ###
### Facebook Mutual Friends ###

--Upload input to HDFS
hadoop fs -mkdir input/facebook
hadoop fs -copyFromLocal /hirw-workshop/input/facebook/* input/facebook

--Delete output directory
hadoop fs -rm -r output/facebook

--Execute MapReduce job
hadoop jar /hirw-workshop/mapreduce/facebook/FacebookFriends-0.0.1.jar com.hirw.facebookfriends.mr.FacebookFriendsDriver  -libjars /hirw-workshop/mapreduce/facebook/json-simple-1.1.jar /user/hirw/input/facebook output/facebook

--Review output
hadoop fs -ls output/facebook
hadoop fs -libjars /hirw-workshop/mapreduce/facebook/FacebookFriends-0.0.1.jar -text output/facebook/part-r-00000