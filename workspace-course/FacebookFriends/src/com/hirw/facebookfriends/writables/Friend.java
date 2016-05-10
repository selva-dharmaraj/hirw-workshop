package com.hirw.facebookfriends.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Friend implements WritableComparable {
	
	private IntWritable id;
	private Text name;
	private Text homeTown;
	
	public Friend() {
		this.id = new IntWritable();
		this.name = new Text();
		this.homeTown = new Text();
	}
	
	public Friend(IntWritable id, Text name, Text homeTown) {
		this.id = id;
		this.name = name;
		this.homeTown = homeTown;
	}
	
	public IntWritable getId() {
		return id;
	}
	public void setId(IntWritable id) {
		this.id = id;
	}
	public Text getName() {
		return name;
	}
	public void setName(Text name) {
		this.name = name;
	}
	public Text getHomeTown() {
		return homeTown;
	}
	public void setHomeTown(Text homeTown) {
		this.homeTown = homeTown;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		name.write(out);
		homeTown.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		name.readFields(in);
		homeTown.readFields(in);
	}
	
	@Override
	public int compareTo(Object o) {
		Friend f2 = (Friend)o;
		return getId().compareTo(f2.getId());
	}
	
	
	@Override
	public boolean equals(Object o) {
		Friend f2 = (Friend)o;
		return getId().equals(f2.getId());
	}

	@Override
	public String toString() {
		//return "Friend [id=" + id + ", name=" + name + ", homeTown=" + homeTown +"]";
		return id + ":" + name+" ";
	}

	
	
}
