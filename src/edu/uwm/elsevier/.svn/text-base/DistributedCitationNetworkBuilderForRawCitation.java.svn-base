/**
 * 
 */
package edu.uwm.elsevier;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import articlesdata.article.Citation;
import articlesdata.database.ArticlesDataDBConnection;
import articlesdata.database.inputformat.DBConfiguration;
import articlesdata.database.inputformat.DataDrivenDBInputFormat;
import articlesdata.database.inputformat.MRJobConfig;

import edu.uwm.elsevier.DistributedCitationNetworkBuilder.Reduce;

/**
 * @author qing
 *
 */
public class DistributedCitationNetworkBuilderForRawCitation extends Configured implements Tool{

private static Logger LOGGER = NetworkBuilderLogger.getLogger("DistributedCitationNetworkBuilderForRawCitation");
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
        
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
        int numMaps = 9000;
        
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        Job job = new Job(conf);
        job.setJarByClass(DistributedDBDrivenCitationNetworkBuilder.class);
        job.setJobName("distributed-db-driven-citation-network-builder-for-raw-citation");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
//        String inputQuery = "select citation_detail.citation_id, title, group_concat(surname SEPARATOR ';') , ssource from citation_detail, citation_author_detail where " + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN
//        		+"AND is_originally_parsed = 1" +
//        		" AND citation_detail.citation_id = citation_author_detail.citation_id" +
//        		" AND (citation_detail.citation_id NOT in (select citation_id from cnetworkv4 where +"+DataDrivenDBInputFormat.SUBSTITUTE_TOKEN +"))";
               String inputQuery = "select citation_qing.citation_id, citation_text " +
               		"from citation_qing, citation_detail" +
               		" where " + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN
        		+" AND is_originally_parsed = 0" +
        		" AND citation_detail.citation_id = citation_qing.citation_id " ;
        
        String inputBoundingQuery = "select min(citation_id), max(citation_id) from citation_qing";
        
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "citation_qing.citation_id");
        DataDrivenDBInputFormat.setInput(job, RawCitation.class, inputQuery, inputBoundingQuery);
        
        DistributedCache.addCacheFile(new URI("/user/qzhang/elsevier_index/"), job.getConfiguration());
        job.setMapperClass(NetworkDBDrivenBuilderForRawCitationMap.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(0);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);

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
            int ret = ToolRunner.run(new DistributedCitationNetworkBuilderForRawCitation(), args);
            System.exit(ret);
        } catch (Exception ex) {
            LOGGER.error(null, ex);
        }
	}

}
