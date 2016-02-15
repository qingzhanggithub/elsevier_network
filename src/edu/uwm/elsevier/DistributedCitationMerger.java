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
import edu.uwm.elsevier.DistributedDBDrivenCitationNetworkBuilder.Reduce;

/**
 * @author qing
 *
 */
public class DistributedCitationMerger extends Configured implements Tool{
	
private static Logger LOGGER = NetworkBuilderLogger.getLogger("DistributedCitationMerger");
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
//        int numMaps = 9000;
        int numMaps = Integer.parseInt(args[1]);
        
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        Job job = new Job(conf);
        job.setJarByClass(DistributedCitationMerger.class);
        job.setJobName("distributed-db-driven-citation-merger");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
//        String inputQuery = "select citation_detail.citation_id, title, group_concat(surname SEPARATOR ';') , ssource from citation_detail, citation_author_detail where " + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN
//        		+"AND is_originally_parsed = 1" +
//        		" AND citation_detail.citation_id = citation_author_detail.citation_id" +
//        		" AND (citation_detail.citation_id NOT in (select citation_id from cnetworkv4 where +"+DataDrivenDBInputFormat.SUBSTITUTE_TOKEN +"))";
               String inputQuery = "select citation_detail.citation_id, title, group_concat(surname SEPARATOR ';') , ssource " +
               		" from citation_detail, citation_author_detail where " + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN
            	+" AND is_originally_parsed = 1 " +
        		" AND citation_detail.citation_id = citation_author_detail.citation_id " +
        		"group by citation_detail.citation_id" ;
        
        String inputBoundingQuery = "select min(citation_id), max(citation_id) from citation_detail";
        
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "citation_detail.citation_id");
        DataDrivenDBInputFormat.setInput(job, Citation.class, inputQuery, inputBoundingQuery);
        LOGGER.debug("Before adding cache file");
        DistributedCache.addCacheFile(new URI("/user/qzhang/elsevier_citation_index3/"), job.getConfiguration());
        LOGGER.debug("Finished adding cache file");
        job.setMapperClass(CitationMergerMap.class);
        job.setCombinerClass(CitationMergerReducer.class);
        job.setReducerClass(CitationMergerReducer.class);
        job.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(CitationCitationMappingStatus.class);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
        
	}

	public static void main(String[] args) {
		try {
            int ret = ToolRunner.run(new DistributedCitationMerger(), args);
            System.exit(ret);
        } catch (Exception ex) {
            LOGGER.error(null, ex);
        }
	}

}
