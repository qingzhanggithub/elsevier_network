/**
 * 
 */
package edu.uwm.elsevier.journal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author qing
 *
 */
public class JournalGrouperReducer extends
		Reducer<IntWritable, JournalRecord, IntWritable, JournalRecord> {

	@Override
	protected void reduce(IntWritable key, Iterable<JournalRecord> values, Context context)
			throws IOException, InterruptedException {
		for(JournalRecord value: values){
			context.write(key, value);
		}
	}
	
}
