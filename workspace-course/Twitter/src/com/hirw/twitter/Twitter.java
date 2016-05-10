
package com.hirw.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;


public class Twitter {

	/** The actual Twitter stream. It's set up to collect raw JSON data */
	private TwitterStream twitterStream;
	private String[] keywords;
	private String propertiesFileLocation;
	private String destinationFileLocation;
	
	Properties prop = new Properties();
	FileOutputStream fos;

	public Twitter(String ploc, String dloc) {
		
		propertiesFileLocation = ploc;
		destinationFileLocation = dloc;

		//load a properties file
		try {
			prop.load(new FileInputStream(propertiesFileLocation));


			ConfigurationBuilder config = new ConfigurationBuilder();
			config.setOAuthConsumerKey(prop.getProperty("CONSUMER_KEY"));
			config.setOAuthConsumerSecret(prop.getProperty("CONSUMER_SECRET"));
			config.setOAuthAccessToken(prop.getProperty("ACCESS_TOKEN"));
			config.setOAuthAccessTokenSecret(prop.getProperty("ACCESS_TOKEN_SECRET"));
			config.setJSONStoreEnabled(true);
			config.setIncludeEntitiesEnabled(true);

			twitterStream = new TwitterStreamFactory(config.build()).getInstance();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void startTwitter() {
		
		try {
			fos = new FileOutputStream(new File(destinationFileLocation));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//Add listener to the stream
		twitterStream.addListener(listener);

		
		String keywordString = prop.getProperty("TWITTER_KEYWORDS");
		keywords = keywordString.split(",");
		for (int i = 0; i < keywords.length; i++) {
			keywords[i] = keywords[i].trim();
		}

		System.out.println("Starting Twitter stream...");

		//Filter only relevant tweets based on the keywords
		FilterQuery query = new FilterQuery().track(keywords);
		twitterStream.filter(query);


	}

	public void stopTwitter() {
		
		System.out.println("Shut down Twitter stream...");
		twitterStream.shutdown();

		try {
			//Close file
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	StatusListener listener = new StatusListener() {

		//The onStatus method is executed every time a new tweet comes in.
		//When we get a tweet, write it to a file
		public void onStatus(Status status) {

			String newLine = "\r\n";
			System.out.println(status.getUser().getScreenName() + ": " + status.getText());

			System.out.println("timestamp : "+ String.valueOf(status.getCreatedAt().getTime()));
			try {
				fos.write(DataObjectFactory.getRawJSON(status).getBytes());
				fos.write(newLine.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		// This listener will ignore everything except for new tweets
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
		public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
		public void onScrubGeo(long userId, long upToStatusId) {}
		public void onException(Exception ex) {}
		public void onStallWarning(StallWarning warning) {}
	};

	public static void main(String[] args) throws InterruptedException {
		
		if(args.length != 2) {
			System.err.println("Usage 2 parameters - 1. properties files with tokes & keywords 2. destination file location");
			System.exit(-1);
		}
		
		Twitter twitter = new Twitter(args[0], args[1]);
		twitter.startTwitter();
		Thread.sleep(300000);
		twitter.stopTwitter();
		
	}

}
