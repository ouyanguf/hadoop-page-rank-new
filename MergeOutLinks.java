package PageRank;

import java.io.*;
import java.util.*;
import java.util.ArrayList;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MergeOutLinks extends Configured {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] parts = line.split("[ \t]");
			if (parts.length <= 0)
				return;
			outputKey.set(parts[0]);
			if (parts.length > 1)
				outputValue.set(parts[1]);
			else
				outputValue.set("");
			output.collect(outputKey, outputValue);

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder outlinkString = new StringBuilder();
			while (values.hasNext()) {
				String v = values.next().toString() + "\t";
				outlinkString.append(v);
			}
			outputValue.set(outlinkString.toString());
			output.collect(key, outputValue);
		}
	}
}
