/**
 * 
 */
package edu.uwm.elsevier.medline.mesh;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author qing
 *
 */
public class MeshNetworkReducer extends
		Reducer<MeshNetworkEdge, IntWritable, MeshNetworkEdge, IntWritable> {

	@Override
	protected void reduce(MeshNetworkEdge key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable value: values){
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
	
	

}
