package PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank extends Configured implements Tool {	
	
	public int run(String[] args) throws Exception {
                
                Path inPath = new Path(args[0]);
                Path outPath = new Path(args[1]);
                Path graphPath = new Path(args[1]+"-Graph-Info");
	        Path countPath = new Path(args[1] + "-Count-N"); 
	        Path iterInPath;// = new Path(" ");
	        Path iterOutPath;// = new Path(" ");
                
                JobConf conf = new JobConf(Extract.class);
                conf.setJobName("ExtractInfo");
	        conf.setMapperClass(Extract.Map.class);
	        conf.setReducerClass(Extract.Reduce.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setInputFormat(XMLInputFormatOld.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        conf.set("xmlinput.start","<page>");
	        conf.set("xmlinput.end","</page>");
	        FileInputFormat.setInputPaths(conf, inPath);
        	FileOutputFormat.setOutputPath(conf, graphPath);
        	JobClient.runJob(conf);

                conf = new JobConf(CountPages.class);                
                conf.setJobName("CountPages");
	        conf.setMapperClass(CountPages.Map.class);
	        conf.setReducerClass(CountPages.Reduce.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        FileInputFormat.setInputPaths(conf, graphPath);
        	FileOutputFormat.setOutputPath(conf, countPath);
        	JobClient.runJob(conf);
        	
        	//JobConf conf = new JobConf(Rank.class);
                FileSystem fs;// = FileSystem.get(conf);
                //Path inpath = new Path(" ");
                //Path outpath = new Path(" ");
                int numIter = 2;
                
                for(int i = 1; i<=numIter; i++){
                        iterInPath = (i == 1) ? graphPath : (new Path("outputOfThe"+String.valueOf(i-1)));
                        iterOutPath = new Path("outputOfThe"+String.valueOf(i));
                        conf = new JobConf(Rank.class);
                  
                        conf.setJobName("Rank Iterations");
		        conf.setMapperClass(Rank.Map.class);
		        conf.setReducerClass(Rank.Reduce.class);
		        conf.setOutputKeyClass(Text.class);
		        conf.setOutputValueClass(Text.class);
		        conf.setInputFormat(TextInputFormat.class);
		        conf.setOutputFormat(TextOutputFormat.class);
		        FileInputFormat.setInputPaths(conf, iterInPath);
	        	FileOutputFormat.setOutputPath(conf, iterOutPath);
	        	JobClient.runJob(conf);
	        	fs = FileSystem.get(conf);
	        	if(i!=1) {
	        	        //fs.delete(new Path("outputOfThe"+String.valueOf(i-1)),true);
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
		FileInputFormat.setInputPaths(conf, new Path("outputOfThe"+String.valueOf(numIter)));
	        FileOutputFormat.setOutputPath(conf, new Path(args[1]+"-Final"));
	        JobClient.runJob(conf);
	        fs = FileSystem.get(conf);
	        //fs.delete(new Path("outputOfThe"+String.valueOf(numIter)),true);
	        		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}

}
