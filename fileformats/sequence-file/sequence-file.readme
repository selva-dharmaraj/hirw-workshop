--Consolidate Small Files to Big Dataset
hadoop jar /hirw-workshop/fileformats/sequence-file/sequencefile-2.0.jar com.hirw.sequencefile.ConvertToSequenceFile /user/hirw/input/fileformats/sequence-file output/fileformats/sequence-file/compressed-sequence-file true

hadoop fs -ls output/fileformats/sequence-file

--Check the blocks
sudo -u hdfs hdfs fsck /user/hirw/output/fileformats/sequence-file/compressed-sequence-file -files -blocks -locations 

hadoop fs -rm -r output/fileformats/sequence-file/maxcloseprice-sequence

--MapReduce program against the sequence file
hadoop jar /hirw-workshop/fileformats/sequence-file/maxcloseprice-sequence-1.0.jar com.hirw.maxcloseprice.sequence.MaxClosePriceSequence /user/hirw/output/fileformats/sequence-file output/fileformats/sequence-file/maxcloseprice-sequence

--Check output
hadoop fs -ls output/fileformats/sequence-file/maxcloseprice-sequence

hadoop fs -cat output/fileformats/sequence-file/maxcloseprice-sequence/part-r-00000

hadoop fs -text output/fileformats/sequence-file/maxcloseprice-sequence/part-r-00000