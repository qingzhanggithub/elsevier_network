/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.NetworkBuilderLogger;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

/**
 * @author qing
 *
 */
public class MeshAnalysisReducer extends
Reducer<IntWritable, MeshEntity, IntWritable, MeshEntity>{
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("MeshAnalysisReducer");
	@Override
	protected void reduce(IntWritable key, Iterable<MeshEntity> values, Context context) {
		Iterator<MeshEntity> itr = values.iterator();
		while(itr.hasNext()){
			MeshEntity entity = itr.next();
			try {
				context.write(key, entity);
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.error("mesh_id:"+key+"\n"+e.getMessage());
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOGGER.error("mesh_id:"+key+"\n"+e.getMessage());
			}
		}
	}

}
