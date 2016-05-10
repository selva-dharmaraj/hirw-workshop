package com.hirw.facebookfriends.tests;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.hirw.facebookfriends.mr.FacebookFriendsMapper;
import com.hirw.facebookfriends.mr.FacebookFriendsReducer;
import com.hirw.facebookfriends.writables.Friend;
import com.hirw.facebookfriends.writables.FriendArray;
import com.hirw.facebookfriends.writables.FriendPair;


public class FacebookFriendsTest {
	
	private List<String> jsonText = new ArrayList<String>();
	
	Friend f1 = populateFriend ("{\"id\":100, \"name\":\"anna\", \"hometown\":\"chicago\"}");
	Friend f2 = populateFriend ("{\"id\":400, \"name\":\"pete\", \"hometown\":\"new jersey\"}");
	Friend f3 = populateFriend ("{\"id\":500, \"name\":\"emily\", \"hometown\":\"san fransisco\"}");
	Friend f4 = populateFriend ("{\"id\":1500, \"name\":\"glenn\", \"hometown\":\"uptown\"}");
	Friend f5 = populateFriend ("{\"id\":300, \"name\":\"jerry\", \"hometown\":\"miami\"}");
	
	MapDriver<LongWritable, Text, FriendPair, FriendArray> facebookMapper;
	ReduceDriver<FriendPair, FriendArray, FriendPair, FriendArray> facebookReducer;
	
	@Before
	public void setUp() {
		
		InputStream in = this.getClass().getResourceAsStream("test.json");
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line;
        try {
			while ((line = reader.readLine()) != null) {
			    jsonText.add(line);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//Setup mapper/reducer/driver
        FacebookFriendsMapper mapper = new FacebookFriendsMapper();
        FacebookFriendsReducer reducer = new FacebookFriendsReducer();

		
		facebookMapper = MapDriver.newMapDriver(mapper);
		facebookReducer = ReduceDriver.newReduceDriver(reducer);
		
	}
	
	@Test
	public void testCompareFriendPair() {
		FriendPair fp1 = new FriendPair(f1, f2);
		FriendPair fp2 = new FriendPair(f2, f1);
		assertEquals(0, fp1.compareTo(fp2));
	}
	
	@Test
	public void testEqualsFriend() {
		assertEquals(f1, f1);
	}
	
	@Test
	public void testEqualsFriendPair() {

		FriendPair fp1 = new FriendPair(f1, f2);
		FriendPair fp2 = new FriendPair(f2, f1);
		
		assertEquals(fp1, fp2);
	}

	@Test
	public void testFacebookFriendsMapper() {
		
		Friend[] farray = new Friend[2];
		farray[0] = f2;
		farray[1] = f3;
		
		facebookMapper.withInput(new LongWritable(), new Text(jsonText.get(0)));
		
		facebookMapper.withOutput(new FriendPair(f1, f2), new FriendArray(Friend.class, farray));
		facebookMapper.withOutput(new FriendPair(f1, f3), new FriendArray(Friend.class, farray));
		try {
			facebookMapper.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testFacebookFriendsReducer() {
		List<FriendArray> values = new ArrayList<FriendArray>();
		
		//anna's friends
		Friend[] farray1 = new Friend[2];
		farray1[0] = f2;
		farray1[1] = f3;
		values.add(new FriendArray(Friend.class, farray1));
		
		//jerry's friends
		Friend[] farray2 = new Friend[2];
		farray2[0] = f3;
		farray2[1] = f4;
		values.add(new FriendArray(Friend.class, farray2));
		
		
		Friend[] farray3 = new Friend[1];
		farray3[0] = f3;
				
		facebookReducer.withInput(new FriendPair(f1,f5), values);
		facebookReducer.withOutput(new FriendPair(f1,f5), new FriendArray(Friend.class, farray3));
		
		try {
			facebookReducer.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return friend;
	}
	
}
