package edu.uwm.elsevier.authoranalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorAnalysisReducer extends
		Reducer<LongWritable, TimeSliceEntity, LongWritable, TimeSliceEntity> {

	@Override
	protected void reduce(LongWritable key, Iterable<TimeSliceEntity> values,Context context)
			throws IOException, InterruptedException {
		for(TimeSliceEntity value: values){
			context.write(key, value);
		}
	}
	
}
