package com.jerry.hadoop.sequencefile;

//cc SequenceFileWriteDemo Writing a SequenceFile
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

// vv SequenceFileWriteDemo
public class SequenceFileWriteDemo {
  
  private static final String[] DATA1 = {
    "One, two, buckle my shoe",
    "Three, four, shut the door"
  };
  
  private static final String[] DATA2 = {
    "Five, six, pick up sticks",
    "Seven, eight, lay them straight"
  };
  
  private static final String[] DATA3 = {
    "Nine, ten, a big fat hen",
    "Nine, ten, a big fat hen - SIX"
  };
  
  public static void main(String[] args) throws IOException {
//	  System.setProperty("HADOOP_USER_NAME", "ubuntu");
//	  System.setProperty("hadoop.job.ugi", "ubuntu");
	  System.setProperty("user.name", "ubuntu");
	  System.out.println(System.getProperty("user.name"));
	  System.out.println(System.getProperty("hadoop.job.ugi"));
	  System.out.println(System.getProperty("HADOOP_USER_NAME"));
    String uri = "hdfs://ec2-54-208-156-204.compute-1.amazonaws.com:9000/user/ubuntu/input/sequence/SequenceFile.seq";
    Configuration conf = new Configuration();
    conf.addResource(new Path("conf/hadoop-cluster.xml"));
//    conf.set("hadoop.job.ugi", "ubuntu");
//    conf.set("HADOOP_USER_NAME", "ubuntu");
//    conf.set("user.name", "ubuntu");
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    if (fs.exists(path))
    {
        fs.delete(path, true);
    }
    LongWritable key = new LongWritable();
    TextArrayWritable values = new TextArrayWritable();
    Text val = new Text();
    SequenceFile.Writer writer = null;
    try {
    	  System.out.println(System.getProperty("user.name"));
      	  System.out.println(System.getProperty("hadoop.job.ugi"));
      	  System.out.println(System.getProperty("HADOOP_USER_NAME"));
      writer = SequenceFile.createWriter(fs, conf, path,
          key.getClass(), values.getClass());
      
      //for (int i = 0; i < 100; i++) {
        key.set(1);

        Text[] arr = new Text[DATA1.length];
        for(int count  = 0 ; count < DATA1.length ; count++) {
        	arr[count] = new Text(DATA1[count]);
        }
        values.set(arr);
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, values);

        writer.append(key, values);
        
        
        key.set(2);

        arr = new Text[DATA2.length];
        for(int count  = 0 ; count < DATA2.length ; count++) {
        	arr[count] = new Text(DATA2[count]);
        }
        values.set(arr);
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, values);
        writer.append(key, values);
        
        
        key.set(1);

        arr = new Text[DATA3.length];
        for(int count  = 0 ; count < DATA3.length ; count++) {
        	arr[count] = new Text(DATA3[count]);
        }
        values.set(arr);
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, values);
        writer.append(key, values);
        
        
      //}
    } finally {
      IOUtils.closeStream(writer);
    }
    

 
/*    
    IntWritable key = new IntWritable();
    Text[] values = new Text[100];
    Text val = new Text();
    SequenceFile.Writer writer = null;
    
    try {
        writer = SequenceFile.createWriter(fs, conf, path,
            key.getClass(), values.getClass());
        
        for (int i = 0; i < 100; i++) {
          key.set(100 - i);

          for(int count  = 0 ; count < DATA.length ; count++) {
        	  values[count] = new Text(DATA[count]);
          }
          
          System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, values);
          writer.append(key, values);
        }
      } finally {
        IOUtils.closeStream(writer);
      }
*/
  }

  
}
// ^^ SequenceFileWriteDemo