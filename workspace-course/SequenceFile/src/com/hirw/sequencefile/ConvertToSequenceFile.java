package com.hirw.sequencefile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;

public class ConvertToSequenceFile {

    /**
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path(args[0]);
        Path outputFile = new Path(args[1]);
        Boolean isCompression = new Boolean(args[2]);
        FSDataInputStream inputStream;
        LongWritable key = new LongWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        
        if(isCompression)
        	writer = SequenceFile.createWriter(fs, conf,
                    outputFile, key.getClass(), value.getClass(), CompressionType.BLOCK, new GzipCodec());        
        else
        	writer = SequenceFile.createWriter(fs, conf,
                outputFile, key.getClass(), value.getClass());
        
        FileStatus[] fStatus = fs.listStatus(inputFile);

        for (FileStatus fst : fStatus) {
            
            System.out.println("Processing file : " + fst.getPath().getName());
            inputStream = fs.open(fst.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            
            int noOfLines = 0;
            
            while(inputStream.available() > 0) {
            	value.set(br.readLine());
            	writer.append(new LongWritable(value.getLength()), value);
            	noOfLines++;
            }
            System.out.println("Number of lines written "+ noOfLines);
            
          }
        fs.close();
        IOUtils.closeStream(writer);
        System.out.println("Sequence File Created Successfully!");
    }
}