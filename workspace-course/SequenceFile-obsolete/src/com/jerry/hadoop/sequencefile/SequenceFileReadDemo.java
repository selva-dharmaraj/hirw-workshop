package com.jerry.hadoop.sequencefile;

//cc SequenceFileReadDemo Reading a SequenceFile
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

// vv SequenceFileReadDemo
public class SequenceFileReadDemo {
  
  public static void main(String[] args) throws IOException {
    
	String uri = "hdfs://192.168.119.128:9000/user/hadoop-user/output/sequence/seqoutput-r-00000";
	//String uri = "hdfs://192.168.119.128:9000/user/hadoop-user/output/sequence/seqinput-r-00000";
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    //    int pathLength = path.toString().length();
    //    System.out.println(path.toString().substring(pathLength-4, pathLength));
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs, path, conf);
      Writable key = (Writable)
        ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable)
        ReflectionUtils.newInstance(reader.getValueClass(), conf);
      

      
      long position = reader.getPosition();
      while (reader.next(key, value)) {
        String syncSeen = reader.syncSeen() ? "*" : "";

        System.out.printf("[%s]\t%s\t%s\n", position, syncSeen, key);

        
        if(value instanceof TextArrayWritable) {
      	  TextArrayWritable tarr = (TextArrayWritable)value;
          for(Writable t : tarr.get()) {
          	Text txt = (Text)t;
          	System.out.println(txt.toString());
          }
        }
        else if(value instanceof HopWritable) {
      	  HopWritable hw = (HopWritable)value;
      	  System.out.printf("%s\t%s\t%s\n", hw.getMessageId(), hw.getLocalServerName(), hw.getLocalServerIP());
        }
        
        	
        position = reader.getPosition(); // beginning of next record
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }
}
// ^^ SequenceFileReadDemo
