package PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;

public class PageRank extends Configured implements Tool {
	static class LinkMultiFileOutput extends
			MultipleTextOutputFormat<Text, Text> {

		protected String generateFileNameForKeyValue(Text key, Text value,
				String name) {
			return "PageRank.inlink.out";
		}
	}

	public int run(String[] args) throws Exception {
		// Extract links and remove red nodes
		Path xmlPath = new Path(args[0]);
		Path rawlinkPath = new Path(args[1] + "-extractLink");
		JobConf conf = new JobConf(Extract.class);
		conf.setJobName("ExtractInfo");
		conf.setMapperClass(Extract.Map.class);
		conf.setReducerClass(Extract.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(XMLInputFormatOld.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		FileInputFormat.setInputPaths(conf, xmlPath);
		FileOutputFormat.setOutputPath(conf, rawlinkPath);
		JobClient.runJob(conf);

		// Merge outlinks
		Path outlinkPath = new Path(args[1] + "-outlink");
		conf = new JobConf(MergeOutLinks.class);
		conf.setJobName("Mergeoutlink");
		conf.setMapperClass(MergeOutLinks.Map.class);
		conf.setReducerClass(MergeOutLinks.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, rawlinkPath);
		FileOutputFormat.setOutputPath(conf, outlinkPath);
		JobClient.runJob(conf);

		// Calculate N
		Path nPath = new Path(args[1] + "-N");
		conf = new JobConf(CountPages.class);
		conf.setJobName("CountPages");
		conf.setMapperClass(CountPages.Map.class);
		conf.setReducerClass(CountPages.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, outlinkPath);
		FileOutputFormat.setOutputPath(conf, nPath);
		JobClient.runJob(conf);

		// Add Initial Rank
		// get N
		String nStr = new Scanner(new File(args[1] + "-N/part-00000"))
				.useDelimiter("\\A").next();
		String[] parts = nStr.split("[ \t]");
		nStr = parts[0].substring(2, parts[0].length());
		int N = Integer.valueOf(nStr);

		Path initRankPath = new Path(args[1] + "-preRank");
		conf = new JobConf(PreRank.class);
		conf.setJobName("Add_Initial_Rank");
		conf.setMapperClass(PreRank.Map.class);
		conf.setReducerClass(PreRank.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setInt("n.count", N);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, outlinkPath);
		FileOutputFormat.setOutputPath(conf, initRankPath);
		JobClient.runJob(conf);

		Path iterInPath;
		Path iterOutPath;
		FileSystem fs;
		int final NUMBER_OF_ITERATIONS = 8;

		for (int i = 1; i <= NUMBER_OF_ITERATIONS; i++) {
			iterInPath = (i == 1) ? initRankPath : (new Path("outputOfIter-"
					+ String.valueOf(i - 1)));
			iterOutPath = new Path("outputOfIter-" + String.valueOf(i));
			conf = new JobConf(Rank.class);
			conf.setJobName("Rank Iterations");
			conf.setMapperClass(Rank.Map.class);
			conf.setReducerClass(Rank.Reduce.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			conf.setInt("n.count", N); // Use N Count
			FileInputFormat.setInputPaths(conf, iterInPath);
			FileOutputFormat.setOutputPath(conf, iterOutPath);
			JobClient.runJob(conf);
			fs = FileSystem.get(conf);
			if (i > 2) {
				// fs.delete(new
				// Path("outputOfIter-"+String.valueOf(i-1)),true);
			}
		}

		conf = new JobConf(SortRank.class);
		conf.setJobName("SortRank");
		conf.setMapperClass(SortRank.Map.class);
		conf.setReducerClass(SortRank.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setInt("n.count", N); // Use N Count
		FileInputFormat.setInputPaths(conf,
				new Path("outputOfIter-" + String.valueOf(numIter)));
		FileOutputFormat.setOutputPath(conf,
				new Path(args[1] + "-Final-Sorted"));
		JobClient.runJob(conf);
		fs = FileSystem.get(conf);
		// fs.delete(new Path("outputOfIter-"+String.valueOf(numIter)),true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}

}
