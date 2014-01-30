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
			outputKey.set("dummy_key");
			
            String line = value.toString();
            String[] parts = line.split("[ \t]");
			
	        for (String part : parts) {
	            outputValue.set(part);
				output.collect(outputKey, outputValue);
	        }	
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		
		private HashSet<String> nodesSet = new HashSet<String>();
		private int n = 0;

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			while (values.hasNext()) {
				String node= values.next().toString();
				nodesSet.add(node);
				n++;
			}
			
            Text outKey = new Text();
            outKey.set("N=" + String.valueOf(nodesSet.size()));
			// outKey.set("N="+String.valueOf(n));
            Text empty = new Text();
            output.collect(outKey, empty);
		}
	}
}
