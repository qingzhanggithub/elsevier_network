/**
 * 
 */
package pmidmapper;

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
import articlesdata.database.ArticlesDataDBConnection;
import articlesdata.database.inputformat.DBConfiguration;
import articlesdata.database.inputformat.DataDrivenDBInputFormat;
import articlesdata.database.inputformat.MRJobConfig;

import edu.uwm.elsevier.ElsevierMedlineMappingStatus;
import edu.uwm.elsevier.NetworkBuilderLogger;

/**
 * @author qing
 *
 */
public class DistributedElsevierMedlineMapping extends Configured implements
		Tool {

	private static Logger LOGGER = NetworkBuilderLogger.getLogger("DistributedElsevierMedlineMapping");
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
//        int numMaps = 9000;
        int numMaps = Integer.parseInt(args[1]);
        
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        Job job = new Job(conf);
        job.setJarByClass(DistributedElsevierMedlineMapping.class);
        job.setJobName("distributed-elsevier-medline-mapping");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
               String inputQuery = "select article_id, title, journal from article where "+DataDrivenDBInputFormat.SUBSTITUTE_TOKEN+
            		   " and source_id=1 ";
        
        String inputBoundingQuery = "select min(article_id), max(article_id) from article";
        
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "article_id");
        DataDrivenDBInputFormat.setInput(job, Article.class, inputQuery, inputBoundingQuery);
        LOGGER.debug("Before adding cache file");
        DistributedCache.addCacheFile(new URI("/user/datauser/pubmed_index/"), job.getConfiguration());
        LOGGER.debug("Finished adding cache file");
        job.setMapperClass(ElsevierMedlineMap.class);
        job.setCombinerClass(ElsevierMedlineReducer.class);
        job.setReducerClass(ElsevierMedlineReducer.class);
        job.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(ElsevierMedlineMappingStatus.class);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
		
	}
	
	public static void main(String[] args) {
		try {
            int ret = ToolRunner.run(new DistributedElsevierMedlineMapping(), args);
            System.exit(ret);
        } catch (Exception ex) {
            LOGGER.error(null, ex);
        }
	}

}
