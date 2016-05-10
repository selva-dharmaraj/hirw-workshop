package com.jerry.hadoop.sequencefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Sequence {

	public static void main(String[] args) {

		Job job;
		try {
			FileSystem fs = FileSystem.get(new Configuration()); 
		    Path outputPath = new Path("/user/hadoop-user/output/sequence");  
		    
		    if (fs.exists(outputPath))
		    {
		    	fs.delete(outputPath, true);
		    }
			
			job = new Job(new Configuration(), "Sequence");

			job.setJarByClass(Sequence.class);
			job.setJobName("Sequence");

			job.setInputFormatClass(CustomInputFormat.class);
			//job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(TextArrayWritable.class);

			job.setOutputKeyClass(LongWritable.class);
			//job.setOutputValueClass(TextArrayWritable.class); //Looks like OK

			job.setMapperClass(SequenceMapper.class);
			job.setReducerClass(SequenceReducer.class);


			FileInputFormat.addInputPath(job, new Path("/user/hadoop-user/input/sequence"));
			FileOutputFormat.setOutputPath(job, outputPath);
			


			 MultipleOutputs.addNamedOutput(job, "seqa",
			   SequenceFileOutputFormat.class,
			   LongWritable.class, HopWritable.class);
			 
			 MultipleOutputs.addNamedOutput(job, "seqb",
					   SequenceFileOutputFormat.class,
					   LongWritable.class, TextArrayWritable.class);
			 
			job.waitForCompletion(true);

		}catch(IOException e) {

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
