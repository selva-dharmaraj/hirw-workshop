package com.jerry.hadoop.sequencefile;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable extends ArrayWritable { 
	
	public TextArrayWritable() { 
		super(Text.class); 
	}
	
	@Override
	public String toString() {
		Text[] textArr = (Text[])this.get();
		StringBuilder sb = new StringBuilder();
		for(Text val : textArr) {
			sb.append(val.toString());
			sb.append("\r\n");
		}
		
		return sb.toString();
	}
}
