package com.hirw.wordcount;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount {

        
 public static void main(String[] args) throws Exception {
	 	    
	Configuration conf = new Configuration();
    
    Job job = new Job(conf, "WordCount");
    job.setJarByClass(com.hirw.wordcount.WordCount.class);
    job.setJobName("WordCount");
    
        
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(com.hirw.wordcount.WordCountMapper.class);
    job.setReducerClass(com.hirw.wordcount.WordCountReducer.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
   
        
    FileInputFormat.addInputPath(job, new Path("/user/hadoop-user/dwp-payments-april10.csv"));
    FileOutputFormat.setOutputPath(job, new Path("/user/hadoop-user/output/wordcount"));
        
    job.waitForCompletion(true);
 }
        
}