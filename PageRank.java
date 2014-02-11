package PageRank;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
// import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.fs.FileUtil;

public class PageRank extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		// Extract links and remove red nodes
		// final String DATA_URL = "s3://spring-2014-ds/data";
		final String DATA_URL = "s3://yahui-dic-pagerank/input";
		final String PATH_PRE = "s3://" + args[0] + "/";
		
		Path xmlPath = new Path(DATA_URL);
		Path rawlinkPath = new Path(PATH_PRE + "tmp/raw-link");
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
		Path outlinkPath = new Path(PATH_PRE + "tmp/out-link");
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
		
		//Write job1 to results dir
		//FileSystem infs = FileSystem.get(URI.create(conf.get("fs.default.name")), conf); 
		FileSystem fs = outlinkPath.getFileSystem(conf);
		FileUtil.copyMerge(fs, outlinkPath, fs,  new Path(PATH_PRE + "results/PageRank.outlink.out"), false, conf, "");

		// Calculate N
		Path nPath = new Path(PATH_PRE + "tmp/number");
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

		fs = nPath.getFileSystem(conf);
		FileUtil.copyMerge(fs, nPath, fs, new Path(PATH_PRE + "results/PageRank.n.out"), false, conf, "");
		// Add Initial Rank
		// get N
		// String nStr = new Scanner(new File(args[1] + "/PageRank.n.out")).useDelimiter("\\A").next();
	// 	String[] parts = nStr.split("[ \t]");
	// 	nStr = parts[0].substring(2, parts[0].length());
   	 	Scanner scanner = new Scanner(fs.open(new Path(PATH_PRE + "results/PageRank.n.out")));
        int N = Integer.parseInt(scanner.nextLine().trim().substring(2));
        // System.out.println("total pages: " + N);
        scanner.close();
		// int N = Integer.valueOf(nStr);

		Path initRankPath = new Path(PATH_PRE + "tmp/preRank");
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

		//Page Rank
		Path iterInPath;
		Path iterOutPath;
		
		final int NUMBER_OF_ITERATIONS = 8;


		for (int i = 1; i <= NUMBER_OF_ITERATIONS; i++) {
			iterInPath = (i == 1) ? initRankPath : (new Path(PATH_PRE + "tmp/outputOfIter-" + String.valueOf(i - 1)));
			iterOutPath = new Path(PATH_PRE + "tmp/outputOfIter-" + String.valueOf(i));
			
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
		}
		
		//sort rank of iteration 1
		conf = new JobConf(SortRank.class);
		conf.setJobName("SortRank iter 1");
		conf.setMapperClass(SortRank.Map.class);
		conf.setReducerClass(SortRank.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setInt("n.count", N); // Use N Count
		FileInputFormat.setInputPaths(conf, new Path(PATH_PRE + "tmp/outputOfIter-1"));
		Path sortedRankIter1OutPath = new Path(PATH_PRE + "tmp/rank-iter1-Sorted");
		FileOutputFormat.setOutputPath(conf, sortedRankIter1OutPath);
		JobClient.runJob(conf);
		
		fs = sortedRankIter1OutPath.getFileSystem(conf);
		FileUtil.copyMerge(fs, sortedRankIter1OutPath, fs, new Path(PATH_PRE + "results/PageRank.iter1.out"), false, conf, "");

		//sort rank of iteration 8
		conf = new JobConf(SortRank.class);
		conf.setJobName("SortRank iter 8");
		conf.setMapperClass(SortRank.Map.class);
		conf.setReducerClass(SortRank.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setInt("n.count", N); // Use N Count
		FileInputFormat.setInputPaths(conf, new Path(PATH_PRE + "tmp/outputOfIter-" + String.valueOf(NUMBER_OF_ITERATIONS)));
		Path sortedRankIter8OutPath = new Path(PATH_PRE + "tmp/rank-iter8-Sorted");
		FileOutputFormat.setOutputPath(conf, sortedRankIter8OutPath);
		JobClient.runJob(conf);
		// fs = FileSystem.get(conf);
		// fs.delete(new Path("outputOfIter-"+String.valueOf(NUMBER_OF_ITERATIONS)),true);
		
		fs = sortedRankIter8OutPath.getFileSystem(conf);
		FileUtil.copyMerge(fs, sortedRankIter8OutPath, fs, new Path(PATH_PRE + "results/PageRank.iter8.out"), false, conf, "");
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}

}
