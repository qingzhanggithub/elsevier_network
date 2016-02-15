/**
 * 
 */
package edu.uwm.elsevier;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author qing
 *
 */
public class CitationMergerReducer extends Reducer<LongWritable, CitationCitationMappingStatus, LongWritable, CitationCitationMappingStatus> {
	
	@Override
	protected void reduce(LongWritable key, Iterable<CitationCitationMappingStatus> values, Context context)throws IOException, InterruptedException {
		Iterator<CitationCitationMappingStatus> itr = values.iterator();
		while(itr.hasNext()){
			CitationCitationMappingStatus status = itr.next();
			context.write(key, status);
		}
	}

}
