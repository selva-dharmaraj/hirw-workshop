package com.hirw.avro;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ColorCountMapper extends
	Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {
	
	@Override
	public void map(AvroKey<User> key, NullWritable value, Context context)
	  throws IOException, InterruptedException {
	
		CharSequence color = key.datum().getFavoriteColor();
		if (color == null) {
		  color = "none";
		}
		context.write(new Text(color.toString()), new IntWritable(1));
	}
}
