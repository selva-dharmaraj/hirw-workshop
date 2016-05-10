package com.hirw.maxcloseprice.tests;

/**
 * MaxTemperatureTest.java
 * www.hadoopinrealworld.com
 * This is a MRUnit test program to test MaxClosePrice MapReduce program
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.hirw.maxcloseprice.MaxClosePriceMapper;
import com.hirw.maxcloseprice.MaxClosePriceReducer;

public class MaxClosePriceTest {

	//Specification of Mapper
	MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	//Specification of Reduce
	ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
	//Specification of MapReduce program
	MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDriver;

	@Before
	public void setUp() {
		MaxClosePriceMapper mapper = new MaxClosePriceMapper();
		MaxClosePriceReducer reducer = new MaxClosePriceReducer();
		//Setup Mapper
		mapDriver = MapDriver.newMapDriver(mapper);
		//Setup Reduce
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		//Setup MapReduce job
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() {
		//Test Mapper with this input
		mapDriver.withInput(new LongWritable(), new Text(
				"ABCSE,ORR,2003-06-30,13.72,13.92,13.15,13.15,859000,12.56"));
		//Expect this output
		mapDriver.withOutput(new Text("ORR"), new FloatWritable((float) 13.15));

		//Test Mapper with this input
		mapDriver.withInput(new LongWritable(), new Text(
				"ABCSE,BLB,2006-08-29,36.60,37.47,36.52,37.42,176700,37.42"));
		//Expect this output
		mapDriver.withOutput(new Text("BLB"), new FloatWritable((float) 37.42));    
		try {
			//Run Map test with above input and ouput
			mapDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testReducer() {
		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable((float) 15.80));
		values.add(new FloatWritable((float) 13.2));
		//Run Reduce with this input
		reduceDriver.withInput(new Text("ORB"), values);
		//Expect this output
		reduceDriver.withOutput(new Text("ORB"), new FloatWritable((float) 15.80));
		try {
			//Run Reduce test with above input and ouput
			reduceDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}