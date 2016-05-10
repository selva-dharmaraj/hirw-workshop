package com.jerry.hadoop.sequencefile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class SequenceReducer extends Reducer<LongWritable, TextArrayWritable, LongWritable, Writable> {
	
	 private MultipleOutputs<LongWritable, Writable> mos;
	 
	 @Override
	 public void setup(Context context) {
		 mos = new MultipleOutputs<LongWritable, Writable>(context);
	 }

	@Override
	public void reduce(LongWritable key, Iterable<TextArrayWritable> values,
			Context context)
					throws IOException, InterruptedException {

		TextArrayWritable value = new TextArrayWritable();
		List<Text> textList = new ArrayList<Text>();

		for(TextArrayWritable tArr : values)  {

			Text[] tempArr = Arrays.copyOf( tArr.get(),  tArr.get().length, Text[].class);
			for(int i = 0 ; i < tempArr.length ; i++)
				textList.add(new Text(tempArr[i]));

		}
		value.set(Arrays.copyOf( textList.toArray(),  textList.toArray().length, Text[].class));
		
		if(value.get().length > 2) {
			HopWritable hop = new HopWritable(1, "jerryserver", "10.100.200.300");
			mos.write("seqa", key, hop, "/user/hadoop-user/output/sequence/seqoutput");
		}

		else
			mos.write("seqb", key, value, "/user/hadoop-user/output/sequence/seqinput");
		
		//context.write(key, value);
	}
}
