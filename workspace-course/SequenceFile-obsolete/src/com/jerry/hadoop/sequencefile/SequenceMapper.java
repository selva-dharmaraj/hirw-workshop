package com.jerry.hadoop.sequencefile;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceMapper extends Mapper<LongWritable, Writable, LongWritable, TextArrayWritable> {

	
	  @Override
	  public void map(LongWritable key, Writable value, Context context)
		      throws IOException, InterruptedException {
		  
		  TextArrayWritable arr = new TextArrayWritable();
		  if(value instanceof Text) {
			  arr.set(new Text[]{(Text)value});
			  if(key.equals(new LongWritable(0)))
				  key = new LongWritable(1);
		  }
		  else
			  arr=(TextArrayWritable)value;
		  
		  context.write(key, arr);

	    
	  }
}
