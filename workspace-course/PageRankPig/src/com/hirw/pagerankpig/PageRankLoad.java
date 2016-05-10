package com.hirw.pagerankpig;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;



public class PageRankLoad extends LoadFunc {

	private static final Log log = LogFactory.getLog(PageRankLoad.class);

	private final TupleFactory tupleFactory = TupleFactory.getInstance();
	private final BagFactory bagFactory = BagFactory.getInstance();
	
	
	@SuppressWarnings("rawtypes")
	private RecordReader reader;

	//HDFS location mentioned in the LOAD instruction will be passed here
	@Override
	public void setLocation(String location, Job job)
			throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	//Return the custome XMLInputFormat
	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new XmlInputFormat();
	}

	//Here we get the reference to the XMLRecordReader
	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	//This method is called for every Page in the Wikipedia dataset
	@Override
	public Tuple getNext() throws IOException {
		try {
			
			Tuple tuple = tupleFactory.newTuple(2);
			DataBag bag = bagFactory.newDefaultBag();
			
					
			if (!reader.nextKeyValue()) {
				return null;
			}
			
			//Wikipedia page
			Text page = (Text) reader.getCurrentValue();
			
			//Get page title and content
			String pageTitle = new String();
	    	String pageContent = new String();
	        
	        int begin = page.find("<title>");
	        int end = page.find("</title>", begin);
	        
	        pageTitle = Text.decode(page.getBytes(), begin+7, end-(begin+7));
	        
	        //Page Title does not look good, ignore.
	        if(pageTitle.contains(":")) 
	            return tuple;

	        begin = page.find(">", page.find("<text"));
	        end = page.find("</text>", begin);
	        
	        if(begin+1 == -1 || end == -1) {
	            pageTitle = "";
	            pageContent = "";
	        }
	        
	        pageContent = Text.decode(page.getBytes(), begin+1, end-(begin+1));
			
	        log.info("Page Title "+pageTitle);

	        tuple.set(0, new DataByteArray(pageTitle.replace(' ', '_').toString()));
	        
	        //Pattern to identify a link
	        Pattern findWikiLinks = Pattern.compile("\\[.+?\\]");

	        Matcher matcher = findWikiLinks.matcher(pageContent);
	        
	        //Go through all the links and get the page 
	        while (matcher.find()) {
	            String linkPage = matcher.group();
	            
	            //Get only wiki page from link
	            linkPage = getPage(linkPage);
	            if(linkPage == null || linkPage.isEmpty()) 
	                continue;

	            //Add the page to the Tuple and add the Tuple to the Bag
	            Tuple ltuple = tupleFactory.newTuple(1);
	            ltuple.set(0, linkPage);
	            bag.add(ltuple);
	            
	        }

	        //Add the Bag to the 2nd column of the Tuple
	        tuple.set(1, bag);
			return tuple;
			
		} catch (InterruptedException e) {
			throw new ExecException(e);
		}
	}
	

	//Get the page from the link
    private String getPage(String linkPage){
    	
    	//Consider only pages from wikipedia. Don't consider links to pages outside of wikipedia 
    	boolean isGoodLink = true;
        int srt = 1;
        if(linkPage.startsWith("[[")){
        	srt = 2;
        }
        
        //Ref. https://en.wikipedia.org/wiki/Help:Link#Wikilinks
        int end = linkPage.indexOf("#") > 0 ? linkPage.indexOf("#") : linkPage.indexOf("|") > 0 ? 
        		linkPage.indexOf("|") : linkPage.indexOf("]") > 0 ? 
        				linkPage.indexOf("]") : linkPage.indexOf("|");
        
        //Weed out bad links
        if( linkPage.length() < srt+2 || linkPage.length() > 100 || linkPage.contains(":") || 
        		linkPage.contains(",") || linkPage.contains("&")) 
        	isGoodLink = false;
        
        char firstChar = linkPage.charAt(srt);
        
        if( firstChar == '#' || firstChar == ',' || firstChar == '.' || firstChar == '\'' || 
        		firstChar == '-' || firstChar == '{') 
        	isGoodLink = false;
    	
        if(!isGoodLink) 
        	return null;
        
        linkPage = linkPage.substring(srt, end);
        //Cleanup link
        linkPage = linkPage.replaceAll("\\s", "_").replaceAll(",", "").replace("&amp;", "&");

        return linkPage;
    }
	
    
 }