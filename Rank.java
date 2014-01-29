package PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Rank extends Configured{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		static enum Counters {INPUT_WORDS}

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private Text thisLink = new Text();
		private double rank = 0.0;
		private int degree = 0;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException {
				
			String line = value.toString();
			String[] parts = line.split("[ \t]");

			thisLink.set(parts[0]);
			if (parts.length >= 2) {
				if (parts[1].length() < 10) { // A rank b c...
					rank = Double.valueOf(parts[1]);
					degree = parts.length - 2;

					StringBuilder builder = new StringBuilder();
					if (parts.length >= 3) {
						for (int i = 2; i < parts.length; i++) {
							builder.append(parts[i] + " ");
						}
					}
					outputValue.set("info " + String.valueOf(degree) + " "
							+ builder.toString());
					output.collect(thisLink, outputValue);

					outputValue.set(thisLink.toString() + " "
							+ String.valueOf(rank) + " "
							+ String.valueOf(degree));
					if (parts.length >= 3) {
						for (int i = 2; i < parts.length; i++) {
							outputKey.set(parts[i]);
							output.collect(outputKey, outputValue);
							reporter.incrCounter(Counters.INPUT_WORDS, 1);
						}
					}
				} else { // A (no rank) b c ...
					rank = 1.0;
					degree = parts.length - 1;

					StringBuilder builder = new StringBuilder();
					if (parts.length >= 2) {
						for (int i = 1; i < parts.length; i++) {
							builder.append(parts[i] + " ");
						}
					}
					outputValue.set("info " + String.valueOf(degree) + " "
							+ builder.toString());
					output.collect(thisLink, outputValue);

					outputValue.set(thisLink.toString() + " "
							+ String.valueOf(rank) + " "
							+ String.valueOf(degree));
					for (int i = 1; i < parts.length; i++) {
						outputKey.set(parts[i]);
						output.collect(outputKey, outputValue);
						reporter.incrCounter(Counters.INPUT_WORDS, 1);
					}
				}
			} else {// only A
				rank = 1.0;
				degree = parts.length - 1;

				outputValue.set("info 0");
				output.collect(thisLink, outputValue);
			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private double factor = 0.85;
		private int nCount = 1;
		private Text outputValue = new Text();
		
		public void configure(JobConf job) {
		        nCount = job.getInt("n.count", 1);
		}
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			double sum = 0.00;
			String outlinks = "";

			while (values.hasNext()) {
				String line = values.next().toString();
				String[] parts = line.split("[ \t]");

				if (!parts[0].equals("info")) {//Not info message
					sum += Double.valueOf(parts[1]) / Double.valueOf(parts[2]);
				} else {
					if (parts.length > 2) {
						for (int i = 2; i < parts.length; i++) {
							outlinks = outlinks.concat(parts[i] + " ");
						}
					}
				}
			}
			sum = factor * sum + (1 - factor) / nCount;
			sum = Math.round(sum * 10000.0) / 10000.0;
			outputValue.set(String.valueOf(sum) + " " + outlinks);
			output.collect(key, outputValue);
		}
	}

}
