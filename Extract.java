package PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Extract extends Configured{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private String getTitle(String page){
			int start = page.indexOf("<title>")+7;
			int end = page.indexOf("</title>");
			if(start>=end) return "NO_TITLE";
			String title = page.substring(start,end).replace(' ','_');
			return title;
		}

		private String getOutlinks(String page){
			StringBuilder outlinks = new StringBuilder();
			StringBuilder text = new StringBuilder(page);
			int textStart = text.indexOf("<text");
			text.delete(0,textStart);
			int textEnd = text.indexOf("</text>");
			text.delete(textEnd+7,text.capacity());
				
			while(text.indexOf("[[")!=-1){
				int linkStart = text.indexOf("[[")+2;
				int linkEnd = text.indexOf("]]",linkStart);
				int colonIndex = text.indexOf(":",linkStart);
				if(colonIndex<linkEnd && colonIndex!=-1){
					text.delete(0,linkEnd+2);
				}else{
					int pipeIndex = text.indexOf("|",linkStart);
					if(pipeIndex<linkEnd && pipeIndex!=-1){
						linkEnd = pipeIndex;
					}
					int sharpIndex = text.indexOf("#",linkStart);
					if(sharpIndex<linkEnd && sharpIndex!=-1){
					        linkEnd = sharpIndex;
					}
					outlinks.append(" "+ text.substring(linkStart,linkEnd).replace(' ','_'));
					text.delete(0,linkEnd+2);
				}
			}

			return outlinks.toString();
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException {
		        String page = value.toString();
			String title = getTitle(page);
			String outlinks = getOutlinks(page);
                        outputKey.set(title);
                        outputValue.set("1"+ outlinks);
                        output.collect(outputKey, outputValue);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		        while (values.hasNext()) {
			        output.collect(key, values.next());
			}
		}
	}

}
