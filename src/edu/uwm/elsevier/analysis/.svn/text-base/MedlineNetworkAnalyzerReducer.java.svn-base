/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.NetworkBuilderLogger;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

/**
 * @author qing
 *
 */
public class MedlineNetworkAnalyzerReducer extends
		Reducer<LongWritable, MedlineAritcleNodeStatistiscs, LongWritable, MedlineAritcleNodeStatistiscs> {
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("MedlineNetworkAnalyzerReducer");
	@Override
	protected void reduce(LongWritable key, Iterable<MedlineAritcleNodeStatistiscs> values, Context context) {
		Iterator<MedlineAritcleNodeStatistiscs> itr = values.iterator();
		while(itr.hasNext()){
			MedlineAritcleNodeStatistiscs stats = itr.next();
			try {
				context.write(key, stats);
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.error(stats.articleId+"\n"+e.getMessage());
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOGGER.error(stats.articleId+"\n"+e.getMessage());
			}
		}
	}

}
