package PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PreRank extends Configured {	
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		private int N = 1;
			
		public void configure(JobConf job) {
		    N = job.getInt("n.count", 1);
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException {
			String line = value.toString();
			String[] parts = line.split("[ \t]");
			
			outputKey.set(parts[0]);
			
			StringBuilder sb = new StringBuilder();
			double r = (double)1/N;
			sb.append(String.valueOf(r));
			for(int i = 1; i < parts.length; i++) {
				sb.append("\t" + parts[i]);
			}
			
			outputValue.set(sb.toString());
			output.collect(outputKey, outputValue);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

            output.collect(key, values.next());
		}
	}
}
