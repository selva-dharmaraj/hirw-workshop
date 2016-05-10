package com.hirw.avro;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class ColorCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
	    if (args.length != 2) {
	        System.err.println("Usage: MapReduceColorCount <input path> <output path>");
	        return -1;
	      }

	      Job job = new Job(getConf());
	      job.setJarByClass(ColorCountDriver.class);
	      job.setJobName("Color Count");

	      FileInputFormat.setInputPaths(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.setInputFormatClass(AvroKeyInputFormat.class);
	      job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
	      
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(IntWritable.class);
	      
	      job.setMapperClass(ColorCountMapper.class);
	      job.setReducerClass(ColorCountReducer.class);

	      //Provide input and output schema for the AVRO file
	      AvroJob.setInputKeySchema(job, User.getClassSchema());
	      AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
	      AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));
	      
	      return (job.waitForCompletion(true) ? 0 : 1);
	    }

	  
	public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(new ColorCountDriver(), args);
		    System.exit(res);
	}
}
