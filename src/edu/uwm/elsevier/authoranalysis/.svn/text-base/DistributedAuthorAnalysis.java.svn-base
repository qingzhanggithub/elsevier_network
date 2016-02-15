/**
 * 
 */
package edu.uwm.elsevier.authoranalysis;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
import edu.uwm.elsevier.CitationCitationMappingStatus;
import edu.uwm.elsevier.CitationMergerMap;
import edu.uwm.elsevier.CitationMergerReducer;
import edu.uwm.elsevier.DistributedCitationMerger;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.namedisambiguation.AuthorityEntity;

/**
 * @author qing
 *
 */
public class DistributedAuthorAnalysis extends Configured implements Tool {
	private Logger logger = Logger.getLogger(DistributedAuthorAnalysis.class);
	
	@Override
	public int run(String[] args) throws Exception {
		logger.info("Task started. Distributed Author Analysis...");
		Configuration conf = getConf();
		
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
        int numMaps = Integer.parseInt(args[1]);
        
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        Job job = new Job(conf);
        job.setJarByClass(DistributedAuthorAnalysis.class);
        job.setJobName("distributed-author-analysis");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
        String inputQuery = "select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP+" where "+DataDrivenDBInputFormat.SUBSTITUTE_TOKEN;
        
        String inputBoundingQuery = "select min(author_id), max(author_id) from "+ITableNames.AUTHORITY_MAP;
        
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "author_id");
        DataDrivenDBInputFormat.setInput(job, AuthorityEntity.class, inputQuery, inputBoundingQuery);
        job.setMapperClass(AuthorAnalysisMapper.class);
        job.setCombinerClass(AuthorAnalysisReducer.class);
        job.setReducerClass(AuthorAnalysisReducer.class);
        job.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(TimeSliceEntity.class);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
        
	}

	public static void main(String[] args) {
		try {
            int ret = ToolRunner.run(new DistributedCitationMerger(), args);
            System.exit(ret);
        } catch (Exception ex) {
            
        }
	}
	
	
	

}
