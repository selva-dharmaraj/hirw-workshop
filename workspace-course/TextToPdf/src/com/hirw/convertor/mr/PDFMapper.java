package com.hirw.convertor.mr;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hirw.convertor.custom.PDFWritable;


public class PDFMapper extends Mapper<Text, PDFWritable, Text, PDFWritable>{

	private static final Log log = LogFactory.getLog(PDFMapper.class);
	String dirName = null;
	String fileName = null;

	@Override
	public void map(Text key, PDFWritable value, Context context) 
			throws IOException, InterruptedException {
		try{
			context.write(key, value);
		}
		catch(Exception e){
			log.info(e);
		}
	}
}