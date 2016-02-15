/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.NetworkBuilderLogger;

/**
 * @author qing
 *
 */
public class MeshAnalysisMap extends Mapper<LongWritable, MeshEntity, IntWritable, MeshEntity>{
	
	protected MeshAnalysis meshAnalysis = null;
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("MeshAnalysisMap");

	@Override
    protected void setup(Context context){
		try {
			meshAnalysis = new MeshAnalysis();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
	}
	
	@Override
	protected void cleanup(Context context){
		if(meshAnalysis !=null)
			try {
				meshAnalysis.closeAllStuff();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	@Override
    public void map(LongWritable k1, MeshEntity v1, Context context){
		try {
			meshAnalysis.getMeshDistOverCitation(v1);
			context.write(new IntWritable(v1.getMeshId()), v1);
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(v1.getMeshId()+":"+e.getMessage());
		} catch (IOException e) {
			LOGGER.error(v1.getMeshId()+":"+e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.error(v1.getMeshId()+":"+e.getMessage());
		} catch (ClassNotFoundException e) {
			LOGGER.error(v1.getMeshId()+":"+e.getMessage());
			e.printStackTrace();
		}
	}
}
