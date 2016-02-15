/**
 * 
 */
package edu.uwm.elsevier.journal;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import articlesdata.article.Article;
import articlesdata.article.Citation;
import articlesdata.database.ArticlesDataDBConnection;
import articlesdata.database.inputformat.DBConfiguration;
import articlesdata.database.inputformat.DataDrivenDBInputFormat;
import articlesdata.database.inputformat.MRJobConfig;
import edu.uwm.elsevier.CitationCitationMappingStatus;
import edu.uwm.elsevier.CitationMergerMap;
import edu.uwm.elsevier.CitationMergerReducer;
import edu.uwm.elsevier.DistributedCitationMerger;
import edu.uwm.elsevier.ITableNames;

/**
 * @author qing
 *
 */
public class DistributedJournalGrouper extends Configured implements Tool {

	private static Logger LOGGER =  Logger.getLogger("DistributedJournalGrouper");
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
        int numMaps = Integer.parseInt(args[1]);
        
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        Job job = new Job(conf);
        job.setJarByClass(DistributedJournalGrouper.class);
        job.setJobName("distributed-db-driven-journal-grouper");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
        String inputQuery = "select * from "+ ITableNames.ARTICLE_TABLE+" where " + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN+" and source_id=1";
        
        String inputBoundingQuery = "select min(article_id), max(article_id) from "+ITableNames.ARTICLE_TABLE;
        
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "article_id");
        DataDrivenDBInputFormat.setInput(job, Article.class, inputQuery, inputBoundingQuery);
        job.setMapperClass(JournalGrouperMapper.class);
        job.setCombinerClass(JournalGrouperReducer.class);
        job.setReducerClass(JournalGrouperReducer.class);
        job.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(JournalRecord.class);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
	}
	
	public static void main(String[] args) {
		try {
            int ret = ToolRunner.run(new DistributedJournalGrouper(), args);
            System.exit(ret);
        } catch (Exception ex) {
            LOGGER.error(null, ex);
        }
	}

}
