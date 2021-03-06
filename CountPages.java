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

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			outputKey.set("dummy_key");
			outputValue.set("dummy_value");
			output.collect(outputKey, outputValue);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private int n = 0;
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				values.next();
				n++;
			}

			outputKey.set("N=" + String.valueOf(n));
			outputValue.set("");
			output.collect(outputKey, outputValue);
		}
	}
}
