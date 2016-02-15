/**
 * 
 */
package edu.uwm.elsevier.prediction.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import pmidmapper.PMArticle;

import articlesdata.database.ArticlesDataDBConnection;
import articlesdata.database.inputformat.DBConfiguration;
import articlesdata.database.inputformat.DataDrivenDBInputFormat;
import articlesdata.database.inputformat.MRJobConfig;
import edu.uwm.elsevier.prediction.AuthorshipEdge;
import edu.uwm.elsevier.prediction.FeatureEntity;

/**
 * @author qing
 *
 */
public class DistributedAuthorPairFeatureExtractor extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(DistributedAuthorPairFeatureExtractor.class);
	
	@Override
	public int run(String[] args) throws Exception {
		logger.debug("Start Distributed-Author-PairFeature-Extractor");
		Configuration conf = getConf();
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	    conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
	    conf.set("class_label", args[1]);
	    int numMaps = Integer.parseInt(args[2]);
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        
        Job job = new Job(conf);
        job.setJarByClass(DistributedAuthorPairFeatureExtractor.class);
        job.setJobName("Distributed-Author-PairFeature-Extractor");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
        String inputQuery = "select * from collab_temp";
        String inputBoundingQuery = "select min(id), max(id) from collab_temp";
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "id");
        DataDrivenDBInputFormat.setInput(job, CollabPair.class, inputQuery, inputBoundingQuery);
        
        DistributedCache.addCacheFile(new URI("/user/qzhang/elsevier_citation_index_2013_v2_official/"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI("/user/qzhang/elsevier_fulltext_index/"), job.getConfiguration());
        DistributedCache.addCacheFile(new URI("/user/datauser/pubmed_index/"), job.getConfiguration());
        
        job.setMapperClass(AuthorPairMapper.class);
        job.setCombinerClass(AuthorPairReducer.class);
        job.setReducerClass(AuthorPairReducer.class);
        job.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(AuthorshipEdge.class);
        job.setOutputValueClass(FeatureEntity.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
	}
	
	public static void main(String[] args) {
        try {
            int ret = ToolRunner.run(new DistributedAuthorPairFeatureExtractor(), args);
            System.exit(ret);
        } catch (Exception ex) {
        	logger.error(null, ex);
        }
    }

}
