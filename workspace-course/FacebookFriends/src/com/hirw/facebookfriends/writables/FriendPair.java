package com.hirw.facebookfriends.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FriendPair implements WritableComparable {
	
	Friend first;
	Friend second;
	
	public FriendPair() {
		this.first = new Friend();
		this.second = new Friend();
	}
	
	public FriendPair(Friend first, Friend second) {
		this.first = first;
		this.second = second;
	}
	
	public Friend getFirst() {
		return first;
	}

	public void setFirst(Friend first) {
		this.first = first;
	}

	public Friend getSecond() {
		return second;
	}

	public void setSecond(Friend second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
    
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int compareTo(Object o) {
		FriendPair pair2 = (FriendPair) o;
		int cmp = -1;
        if(getFirst().compareTo(pair2.getFirst()) == 0 || getFirst().compareTo(pair2.getSecond()) == 0)
        	cmp = 0;
        
        if (cmp != 0) {
                return cmp;
        }
        cmp = -1;
        if(getSecond().compareTo(pair2.getSecond()) == 0 ||  getSecond().compareTo(pair2.getFirst()) == 0)
        	cmp = 0;// reverse
        	
        return cmp;
	}

	@Override
	public boolean equals(Object o) {
        FriendPair pair2 = (FriendPair) o;
        boolean eq =  getFirst().equals(pair2.getFirst()) || getFirst().equals(pair2.getSecond());
        if(!eq)
        	return eq;
        
        return getSecond().equals(pair2.getSecond()) || getSecond().equals(pair2.getFirst());
	}

	@Override
	public String toString() {
		return "[" + first + ";" + second + "]";
	}	
	
	@Override
	public int hashCode() {
		return first.getId().hashCode() + second.getId().hashCode();
	}
	
}
