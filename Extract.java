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

public class Extract extends Configured {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private String getTitle(String page) {
			int start = page.indexOf("<title>");
			if(start == -1) return "";
			start += 7;
			int end = page.indexOf("</title>");
			if (end == -1)
				return "";
			String title = page.substring(start, end).replace(' ', '_');
			return title;
		}

		private HashSet<String> getOutlinks(String page) {
			int textStart = page.indexOf("<text");
			int textEnd = page.indexOf("</text>");
			if (textStart < 0 || textEnd < 0)
				return null;
			String text = page.substring(textStart, textEnd);

			HashSet<String> outlinks = new HashSet<String>();
			int start = 0;

			while (text.indexOf("[[", start) != -1) {
				int linkStart = text.indexOf("[[", start) + 2;
				int linkEnd = text.indexOf("]]", linkStart);
				if (linkEnd < 0)
					break;
				start = linkEnd + 2;
				String pLink = text.substring(linkStart, linkEnd);
				int colonIndex = pLink.indexOf(":");
				int sharpIndex = pLink.indexOf("#");
				if (colonIndex == -1 && sharpIndex == -1) {
					int pipeIndex = pLink.indexOf("|");
					if (pipeIndex != -1) {
						pLink = pLink.substring(0, pipeIndex);
					}
					if(pLink.trim().length() > 0 ) 
						outlinks.add(pLink.replace(' ', '_'));
				}
			}
			return outlinks;
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String page = value.toString();
			String title = getTitle(page);
			// out put the title
			if (title==null || title.isEmpty())
				return;
			outputKey.set(title);
			outputValue.set("!@#TITLE#@!");
			output.collect(outputKey, outputValue);

			HashSet<String> outlinks = getOutlinks(page);
			if (outlinks == null)
				return;
			outputValue.set(title);
			for (String link : outlinks) {
				outputKey.set(link);
				output.collect(outputKey, outputValue);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			boolean haveTitle = false;
			ArrayList<String> links = new ArrayList<String>();
			while (values.hasNext()) {
				String v = values.next().toString();
				if (v.equals("!@#TITLE#@!")) {
					haveTitle = true;
				} else {
					links.add(v);
				}
			}
			if (haveTitle == false)
				return;

			if (links.size() <= 0) { // has no links
				output.collect(key, null);
				return;
			}

			outputValue.set(key.toString());
			for (String link : links) {
				outputKey.set(link);
				output.collect(outputKey, outputValue);
			}
		}
	}
}
