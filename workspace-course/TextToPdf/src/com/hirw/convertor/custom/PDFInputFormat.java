package com.hirw.convertor.custom;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PDFInputFormat extends FileInputFormat<Text, PDFWritable> {

	private static final Log log = LogFactory.getLog(PDFInputFormat.class);

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, PDFWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
		PDFRecordReader reader = new PDFRecordReader();
		reader.initialize(inputSplit, context);
		return reader;
	}

	public class PDFRecordReader extends
	RecordReader<Text, PDFWritable> {

		private FileSplit split;
		private Configuration conf;

		private Text currKey = null;
		private PDFWritable currValue = null;
		private boolean fileProcessed = false;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.split = (FileSplit)split;
			this.conf = context.getConfiguration();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {     
			
			if ( fileProcessed ){
				return false;
			}

			int fileLength = (int)split.getLength();
			byte [] result = new byte[fileLength];

			FileSystem  fs = FileSystem.get(conf);
			FSDataInputStream in = null; 
			try {
				
				in = fs.open( split.getPath());
				IOUtils.readFully(in, result, 0, fileLength);
				   
			} finally {
				IOUtils.closeStream(in);
			}
			this.fileProcessed = true;

			Path file = split.getPath();
			this.currKey = new Text(file.getName());
			this.currValue = new PDFWritable(result);
			
			return true;
		}

		@Override
		public Text getCurrentKey() throws IOException,
		InterruptedException {
			return currKey;
		}

		@Override
		public PDFWritable getCurrentValue() throws IOException,
		InterruptedException {
			return currValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void close() throws IOException {
			// nothing to close
		}

	}        

}