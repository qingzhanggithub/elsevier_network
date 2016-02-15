/**
 * 
 */
package edu.uwm.elsevier.analysis;

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

import pmidmapper.PMArticle;
import articlesdata.database.ArticlesDataDBConnection;
import articlesdata.database.inputformat.DBConfiguration;
import articlesdata.database.inputformat.DataDrivenDBInputFormat;
import articlesdata.database.inputformat.MRJobConfig;

import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.NetworkBuilderLogger;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

/**
 * @author qing
 *
 */
public class DistributedMeshAnalysis extends Configured implements Tool{
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("DistributedMeshAnalysis");

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
            int ret = ToolRunner.run(new DistributedMeshAnalysis(), args);
            System.exit(ret);
        } catch (Exception ex) {
            LOGGER.error(null, ex);
        }

	}

	@Override
	public int run(String[] args) throws Exception {
		LOGGER.debug("Start distributed-mesh-analyzer");
		Configuration conf = getConf();
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
        int numMaps = Integer.parseInt(args[1]);
        int numReducers = Integer.parseInt(args[2]);
        
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        Job job = new Job(conf);
        job.setJarByClass(DistributedMeshAnalysis.class);
        job.setJobName("distributed-mesh-analyzer");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
        String inputQuery = "select mesh_id, mesh, article_ids from "+ITableNames.MESH_ARTICLE_ID_MAP+" where " + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN;
        
        String inputBoundingQuery = "select min(mesh_id), max(mesh_id) from "+ITableNames.MESH_ARTICLE_ID_MAP;
        
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "mesh_id");
        DataDrivenDBInputFormat.setInput(job, MeshEntity.class, inputQuery, inputBoundingQuery);
        
        job.setMapperClass(MeshAnalysisMap.class);
        job.setCombinerClass(MeshAnalysisReducer.class);
        job.setReducerClass(MeshAnalysisReducer.class);
        job.setNumReduceTasks(numReducers);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MeshEntity.class);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
		
	}

}
