package com.hirw.facebookfriends.writables;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class FriendArray extends ArrayWritable {


	public FriendArray() {
		super(Friend.class);
	}

	public FriendArray(Class<? extends Writable> valueClass) {
		super(valueClass);
	}
	
	public FriendArray(Class<? extends Writable> valueClass, Writable[] values) {
	    super(valueClass, values);
	}

	@Override
	public String toString() {

		Friend[] friendArray = Arrays.copyOf(get(), get().length, Friend[].class);
		String print="";
		
		for(Friend f : friendArray) 
			print+=f;
		
		return print;
	}
	
	@Override
	public boolean equals(Object o) {
		Friend[] f1 = Arrays.copyOf(this.get(), this.get().length, Friend[].class);
		Friend[] f2 = Arrays.copyOf(((ArrayWritable) o).get(), ((ArrayWritable) o).get().length, Friend[].class);
		
		boolean result = false;
		
		for(Friend outerf : f1) {
			result = false;
			for(Friend innerf : f2) {
				if(outerf.equals(innerf)) {
					result=true;
					break;
				}
			}
			if(!result)
				return result;
		}
		
		return result;
	}

}
