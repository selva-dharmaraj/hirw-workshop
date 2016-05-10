--Register datafu library
REGISTER '/hirw-workshop/pig/songs/datafu-0.0.4.jar';
DEFINE haversineMiles datafu.pig.geo.HaversineDistInMiles();

songs = LOAD '/user/hirw/input/songs' AS (artist_familiarity:double, artist_lat:double, artist_long:double, artist_name:chararray, song_hotness:double, song_title:chararray);

--keep only clean records and only less familiar artists with great songs
less_popular = FILTER songs BY (artist_familiarity IS NOT NULL and artist_familiarity < 0.5) AND
    (song_hotness IS NOT NULL and song_hotness > 0.5) AND (artist_lat IS NOT NULL) AND (artist_long IS NOT NULL);		
	
grp_less_popular = GROUP less_popular BY (artist_familiarity, artist_lat, artist_long, artist_name);

--an artist might have more than one song, this will avoid duplicates	
avg_song_hotness = FOREACH grp_less_popular GENERATE group.artist_familiarity, group.artist_lat, group.artist_long, group.artist_name, AVG(less_popular.song_hotness) AS avgsong_hotness;

only_hot_song_artists = FILTER avg_song_hotness BY avgsong_hotness > 0.5;

part1 = FOREACH only_hot_song_artists GENERATE
        CONCAT('[', (chararray) artist_lat), artist_long, CONCAT('"', CONCAT((chararray) artist_name, '"')),
		artist_familiarity, CONCAT((chararray)avgsong_hotness, '],');
		
STORE part1 INTO 'output/songs/part1' using PigStorage(',');

--project the artists again to cross the dataset
only_hot_song_artists2 = FOREACH only_hot_song_artists GENERATE artist_familiarity as artist_familiarity2, artist_lat as artist_lat2, artist_long as artist_long2, artist_name as artist_name2, avgsong_hotness as avgsong_hotness2;

--cross both datasets
crossed = CROSS only_hot_song_artists, only_hot_song_artists2;

--remove records with same artists on both sides
only_different = FILTER crossed BY artist_name != artist_name2 AND artist_lat != artist_lat2 AND artist_long != artist_long2;

--use haversineMiles to calculate the distance between two artists
distance = FOREACH only_different GENERATE only_hot_song_artists::artist_lat as artist_lat, only_hot_song_artists::artist_long as artist_long, only_hot_song_artists::artist_name as artist_name, only_hot_song_artists::artist_familiarity as artist_familiarity, only_hot_song_artists::avgsong_hotness as avgsong_hotness,
haversineMiles(only_hot_song_artists::artist_lat, only_hot_song_artists::artist_long, only_hot_song_artists2::artist_lat2, only_hot_song_artists2::artist_long2) as distance;

--group and calculate the average distance between artists
group_artist = GROUP distance BY artist_lat..avgsong_hotness;
avg_distance = FOREACH group_artist GENERATE FLATTEN(group), AVG(distance.distance) AS distanceAvg;

--order and pick the remote artist
desc_order = ORDER avg_distance BY distanceAvg DESC;
top1 = LIMIT desc_order 1;

part2 = FOREACH top1 GENERATE
        CONCAT('[', (chararray) artist_lat), artist_long, CONCAT('"', CONCAT((chararray) artist_name, '"')),
		artist_familiarity, CONCAT((chararray)avgsong_hotness, '],');

STORE part2 INTO 'output/songs/part2' using PigStorage(',');