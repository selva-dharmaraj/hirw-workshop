package com.hirw.convertor.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hirw.convertor.custom.PDFInputFormat;
import com.hirw.convertor.custom.PDFOutputFormat;
import com.hirw.convertor.custom.PDFWritable;

public class TextToPdf {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: warning count <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "text-to-pdf");
		job.setJarByClass(TextToPdf.class);
		job.setMapperClass(PDFMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PDFWritable.class);
		job.setInputFormatClass(PDFInputFormat.class);
		job.setOutputFormatClass(PDFOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}