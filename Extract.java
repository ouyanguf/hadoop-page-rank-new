/**
 * @author: OU Yang & HAN Yahui
 */

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
			//StringBuilder text = new StringBuilder(page);
			int textStart = page.indexOf("<text")+6;
			//text.delete(0,textStart);
			int textEnd = page.indexOf("</text>");
			//text.delete(textEnd,text.capacity());
				
			while(textStart<textEnd){ //text.indexOf("[[")!=-1){
				int linkStart = page.indexOf("[[",textStart)+2;
				int linkEnd = page.indexOf("]]",linkStart);
				
				textStart = linkEnd + 2;//Update text start
				
				if(linkStart == 1 || linkEnd == -1) break; //No [[ or ]] found
				
				if(linkStart == linkEnd) continue;//no content between [[ and ]]

				String link = page.substring(linkStart,linkEnd); //One link
				if(link.contains(":") || link.contains("#"))	continue; //exclude invalid link
				
				int pipeIndex = link.indexOf("|");
				if(pipeIndex == 0) continue;//no content before |
				else if(pipeIndex != -1) link = link.substring(0,pipeIndex);//have pipe in the link
				outlinks.append(" " + link.replace(" ","_"));//append to string builder
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
