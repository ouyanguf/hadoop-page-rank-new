package PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Rank extends Configured {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		static enum Counters {
			INPUT_WORDS
		}

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private Text thisLink = new Text();
		private double rank = 0.0;
		private int degree = 0;
		private int nCount = 1;

		public void configure(JobConf job) {
			nCount = job.getInt("n.count", 1);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString(); // A Rank B C ...
			String[] parts = line.split("[ \t]");

			thisLink.set(parts[0]); // Extract A
			
			if(parts[0].length() <= 0) return;

			rank = Double.valueOf(parts[1]); // Extract Rank

			degree = parts.length - 2; // Calculate Degree

			StringBuilder builder = new StringBuilder();
			if (parts.length > 2) {
				for (int i = 2; i < parts.length; i++) {
					builder.append(parts[i]);
					if(i < parts.length-1) builder.append("\t");
				}
			}
			String v = "!@#info#@!\t" + String.valueOf(degree);
			if(builder.toString().length() > 0)	
				v += "\t" + builder.toString();
			outputValue.set(v);
			output.collect(thisLink, outputValue);

			outputValue.set(thisLink.toString() + "\t" + String.valueOf(rank)
					+ "\t" + String.valueOf(degree));
			if (parts.length > 2) {
				for (int i = 2; i < parts.length; i++) {
					outputKey.set(parts[i]);
					output.collect(outputKey, outputValue);
					reporter.incrCounter(Counters.INPUT_WORDS, 1);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private final double factor = 0.85;
		private int nCount = 1;
		private Text outputValue = new Text();

		public void configure(JobConf job) {
			nCount = job.getInt("n.count", 1);
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			double sum = 0.00;
			StringBuilder outlinks = new StringBuilder();

			while (values.hasNext()) {
				String line = values.next().toString();
				String[] parts = line.split("\\t");

				if (!parts[0].equals("!@#info#@!")) {// Not info message
					sum += Double.valueOf(parts[1]) / Double.valueOf(parts[2]);
				} else { // Info message
					if (parts.length > 2) {
						for (int i = 2; i < parts.length; i++) {
							outlinks.append(parts[i]);
							if(i < parts.length-1) outlinks.append("\t");
						}
					}
				}
			}
			sum = factor * sum + (1 - factor) / nCount;
			//sum = Math.round(sum * 100000.0) / 100000.0;
			outputValue.set(String.valueOf(sum) + "\t" + outlinks.toString());
			if((key!=null) && (key.toString().length() > 0))
				output.collect(key, outputValue);
		}
	}

}
