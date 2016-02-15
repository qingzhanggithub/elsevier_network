/**
 * 
 */
package edu.uwm.elsevier.medline.mesh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.omg.CORBA._IDLTypeStub;

import pmidmapper.PMArticle;

import edu.uwm.elsevier.DistributedCitationMerger;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.medline.CitationNetworkEdge;

import articlesdata.database.ArticlesDataDBConnection;
import articlesdata.database.inputformat.DBConfiguration;
import articlesdata.database.inputformat.DataDrivenDBInputFormat;
import articlesdata.database.inputformat.MRJobConfig;

/**
 * @author qing
 *
 */
public class DistributedMeshNetworkBuilder extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(DistributedMeshNetworkBuilder.class);
	
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		logger.debug("Start distributed-mesh-network-builder");
		Configuration conf = getConf();
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	    conf.set("mapred.child.java.opts", "-Xmx4g -Dfile.encoding=UTF-8");
	    int numMaps = Integer.parseInt(args[1]);
        int numReducers = Integer.parseInt(args[2]);
        conf.setInt(MRJobConfig.NUM_MAPS, numMaps);
        
        Job job = new Job(conf);
        job.setJarByClass(DistributedMeshNetworkBuilder.class);
        job.setJobName("Distributed-Mesh-Network-Builder");
        job.setInputFormatClass(DataDrivenDBInputFormat.class);
        
        DBConfiguration.configureDB(job.getConfiguration(), 
                ArticlesDataDBConnection.MYSQL_DRIVER_CLASS, ArticlesDataDBConnection.URL, 
                ArticlesDataDBConnection.USERNAME, ArticlesDataDBConnection.PASSWORD);
        
        String inputQuery = "select src_article_id, dest_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+
        		" where "+DataDrivenDBInputFormat.SUBSTITUTE_TOKEN;
        String inputBoundingQuery = "select min(src_article_id), max(src_article_id) from "+ITableNames.MEDLINE_NETWORK_TABLE;
        job.getConfiguration().set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "src_article_id");
        DataDrivenDBInputFormat.setInput(job, CitationNetworkEdge.class, inputQuery, inputBoundingQuery);
        
        job.setMapperClass(MeshNetworkMapper.class);
        job.setCombinerClass(MeshNetworkReducer.class);
        job.setReducerClass(MeshNetworkReducer.class);
        job.setNumReduceTasks(numReducers);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(MeshNetworkEdge.class);
        job.setOutputValueClass(IntWritable.class);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
	}
	
	public static void main(String[] args) {
		try {
            int ret = ToolRunner.run(new DistributedMeshNetworkBuilder(), args);
            System.exit(ret);
        } catch (Exception ex) {
            logger.error(null, ex);
        }
	}

}
