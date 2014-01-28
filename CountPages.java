package PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CountPages extends Configured {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private Text thisLink = new Text();
		private double rank = 0.0;
		private int degree = 0;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException {

			outputKey.set("Number of Nodes:");
			//outputValue.set(String.valueOf(degree));
			output.collect(outputKey, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private Text outputValue = new Text();
		private int numNodes = 0;

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			while (values.hasNext()) {
				numNodes++;
				values.next();	
			}
                                Text out = new Text();
                                out.set(String.valueOf(numNodes));
                                output.collect(key, out);
		}
	}
}
