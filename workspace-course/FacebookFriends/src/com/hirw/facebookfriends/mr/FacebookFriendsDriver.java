package com.hirw.facebookfriends.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hirw.facebookfriends.writables.FriendArray;
import com.hirw.facebookfriends.writables.FriendPair;

public class FacebookFriendsDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: fberature <input path> <output path>");
			System.exit(-1);
		}

		//Job Setup
		Job fb = Job.getInstance(getConf(), "facebook-friends");
		
		fb.setJarByClass(FacebookFriendsDriver.class);
		
		
		//File Input and Output format
		FileInputFormat.addInputPath(fb, new Path(args[0]));
		FileOutputFormat.setOutputPath(fb, new Path(args[1]));
		
		fb.setInputFormatClass(TextInputFormat.class);
		fb.setOutputFormatClass(SequenceFileOutputFormat.class);

		//Mapper-Reducer-Combiner specifications
		fb.setMapperClass(FacebookFriendsMapper.class);
		fb.setReducerClass(FacebookFriendsReducer.class);
		
		fb.setMapOutputKeyClass(FriendPair.class);
		fb.setMapOutputValueClass(FriendArray.class);

		//Output key and value
		fb.setOutputKeyClass(FriendPair.class);
		fb.setOutputValueClass(FriendArray.class);
		
		//Submit job
		return fb.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new FacebookFriendsDriver(), args);
		System.exit(exitCode);
	}

}
