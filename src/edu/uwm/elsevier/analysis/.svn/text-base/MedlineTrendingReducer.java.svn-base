/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.NetworkBuilderLogger;

/**
 * @author qing
 *
 */
public class MedlineTrendingReducer extends
		Reducer<IntWritable, MEDLINEArticleTrendingEntity, IntWritable, MEDLINEArticleTrendingEntity> {
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("MedlineTrendingReducer");
	@Override
	protected void reduce(IntWritable key, Iterable<MEDLINEArticleTrendingEntity> values, Context context){
		Iterator<MEDLINEArticleTrendingEntity> itr = values.iterator();
		while(itr.hasNext()){
			try {
				context.write(key, itr.next());
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
			}
		}
	}

}
