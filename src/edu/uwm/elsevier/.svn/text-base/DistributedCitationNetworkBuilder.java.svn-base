package edu.uwm.elsevier;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class DistributedCitationNetworkBuilder extends Configured implements Tool{
	 private static final Logger LOGGER = Logger.getLogger("DistributedCitationNetworkBuilder");

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();// new Configuration();
		 conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		 conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
	        Job job = new Job(conf);
	        job.setJarByClass(DistributedCitationNetworkBuilder.class);
	        job.setJobName("distributed-citation-network-builder-v3_10");
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);//TODO CHECK !!
	        
	        LOGGER.info("Starting distributed citation network builder.");
	        DistributedCache.addCacheFile(new URI("/user/qzhang/elsevier_index/"), job.getConfiguration());
	        job.setMapperClass(NetworkBuilderMap.class);
	        job.setCombinerClass(Reduce.class);
	        job.setReducerClass(Reduce.class);
	        job.setNumReduceTasks(0);

	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        
	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        boolean success = job.waitForCompletion(true);
	        return success ? 0 : 1;
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>  {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            // nothing to do!
        }
    }
	
	public static void main(String[] args) {
        try {
            int ret = ToolRunner.run(new DistributedCitationNetworkBuilder(), args);
            System.exit(ret);
        } catch (Exception ex) {
            LOGGER.error(null, ex);
        }
    }

}
