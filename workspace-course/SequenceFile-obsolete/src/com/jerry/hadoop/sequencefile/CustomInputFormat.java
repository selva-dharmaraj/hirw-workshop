package com.jerry.hadoop.sequencefile;


import java.io.IOException;







import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

public class CustomInputFormat extends FileInputFormat<Writable, Writable>
{

	@Override
	public RecordReader createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
		FileSplit fs = (FileSplit)split;
		
		String inputPath = fs.getPath().toString();
	    int pathLength = inputPath.length();
	    String fileExt = inputPath.substring(pathLength-4, pathLength);
	    if(fileExt.equalsIgnoreCase(".seq"))
			return new SequenceFileRecordReader();
	    
	    return new LineRecordReader();
	}
	


}
