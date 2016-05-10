package com.hirw.facebookfriends.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hirw.facebookfriends.writables.Friend;
import com.hirw.facebookfriends.writables.FriendArray;
import com.hirw.facebookfriends.writables.FriendPair;

public class FacebookFriendsMapper extends Mapper<LongWritable, Text, FriendPair, FriendArray> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		Logger log = Logger.getLogger(FacebookFriendsMapper.class);
		
		StringTokenizer st = new StringTokenizer(value.toString(), "\t");
		String person = st.nextToken();
		String friends = st.nextToken();
		
		Friend f1 = populateFriend(person);
		List<Friend> friendList = populateFriendList(friends);
		Friend[] friendArray = Arrays.copyOf(friendList.toArray(), friendList.toArray().length, Friend[].class);
		FriendArray farray = new FriendArray(Friend.class, friendArray);
		
		for(Friend f2 : friendList) {
			FriendPair fpair = new FriendPair(f1, f2);
			context.write(fpair, farray);
			log.info(fpair+"......"+ farray);
		}

		
	}
	
	private Friend populateFriend(String friendJson) {
		
		
		JSONParser parser = new JSONParser();
		Friend friend = null;
		try {
			
			Object obj = (Object)parser.parse(friendJson);
			JSONObject jsonObject = (JSONObject) obj;

			Long lid = (long)jsonObject.get("id");
			IntWritable id = new IntWritable(lid.intValue());
			Text name = new Text((String)jsonObject.get("name"));
			Text hometown = new Text((String)jsonObject.get("hometown"));
			friend = new Friend(id, name, hometown);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return friend;
	}
	
	private List<Friend> populateFriendList(String friendsJson) {
		
		List<Friend> friendList = new ArrayList<Friend>();
		
		try {
			JSONParser parser = new JSONParser();
			Object obj = (Object)parser.parse(friendsJson.toString());
			JSONArray jsonarray = (JSONArray) obj;

			for(Object jobj : jsonarray) {
				JSONObject entry = (JSONObject)jobj;
				Long lid = (long)entry.get("id");
				IntWritable id = new IntWritable(lid.intValue());
				Text name = new Text((String)entry.get("name"));
				Text hometown = new Text((String)entry.get("hometown"));
				Friend friend = new Friend(id, name, hometown);
				friendList.add(friend);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return friendList;
	}

}
