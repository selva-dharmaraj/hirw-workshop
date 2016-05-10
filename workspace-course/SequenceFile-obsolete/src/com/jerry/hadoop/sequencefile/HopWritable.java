package com.jerry.hadoop.sequencefile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HopWritable implements Writable {
	
	private LongWritable messageId;
	private Text localServerName;
	private Text localServerIP;


	public HopWritable() {
		setMessageId(0);
		setLocalServerName("localhost");
		setLocalServerIP("0.0.0.0");
		
	}
	
	public HopWritable(long messageId, String localServerName, String localServerIP) {
		setMessageId(messageId);
		setLocalServerName(localServerName);
		setLocalServerIP(localServerIP);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		messageId.write(out);
		localServerName.write(out);
		localServerIP.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		messageId.readFields(in);
		localServerName.readFields(in);
		localServerIP.readFields(in);
		
	}
	
	public long getMessageId() {
		return messageId.get();
	}
	
	
	public String getLocalServerName() {
		return localServerName.toString();
	}

	public String getLocalServerIP() {
		return localServerIP.toString();
	}

	public void setMessageId(long messageId) {
		this.messageId = new LongWritable(messageId);
	}

	public void setLocalServerName(String localServerName) {
		this.localServerName = new Text(localServerName);
	}
	
	public void setLocalServerIP(String localServerIP) {
		this.localServerIP = new Text(localServerIP);
	}
	
}
