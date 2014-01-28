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
                
                Path inpath = new Path(args[0]);
                Path outpath = new Path(args[1]);
                
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

	        FileInputFormat.setInputPaths(conf, inpath);
        	FileOutputFormat.setOutputPath(conf, outpath);
        	
        	JobClient.runJob(conf);

                conf = new JobConf(CountPages.class);
                
                conf.setJobName("CountPages");
	        conf.setMapperClass(CountPages.Map.class);
	        conf.setReducerClass(CountPages.Reduce.class);
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(conf, outpath);
	        Path out2 = new Path(args[1] + "Count");
        	FileOutputFormat.setOutputPath(conf, out2);
        	
        	JobClient.runJob(conf);
        			
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}

}
