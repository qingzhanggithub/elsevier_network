/**
 * 
 */
package pmidmapper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.ElsevierMedlineMappingStatus;
import edu.uwm.elsevier.MappingStatus;
import edu.uwm.elsevier.NetworkBuilderLogger;

/**
 * @author qing
 *
 */
public class ElsevierMedlineReducer extends
		Reducer<IntWritable, ElsevierMedlineMappingStatus, IntWritable, ElsevierMedlineMappingStatus> {
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("ElsevierMedlineReducer");
	@Override
	protected void reduce(IntWritable key, Iterable<ElsevierMedlineMappingStatus> values, Context context)throws IOException, InterruptedException {
		Iterator<ElsevierMedlineMappingStatus> itr = values.iterator();
		while(itr.hasNext()){
			ElsevierMedlineMappingStatus status = itr.next();
			LOGGER.info("writting out "+status.getArticleId());
			context.write(key, status);
		}
	}

}
