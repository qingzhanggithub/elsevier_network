package edu.uwm.elsevier.authoranalysis;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.namedisambiguation.AuthorityEntity;


public class AuthorAnalysisMapper extends Mapper<LongWritable, AuthorityEntity, LongWritable, TimeSliceEntity> {
	private TimeSlice timeSlice;
	private Logger logger = Logger.getLogger(AuthorAnalysisMapper.class);

	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		try {
			timeSlice = new TimeSlice();
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		if(timeSlice != null){
			try {
				timeSlice.closeAllStuff();
			} catch (SQLException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void map(LongWritable key, AuthorityEntity value,Context context)throws IOException, InterruptedException {
		try {
			List<TimeSliceEntity> entities = timeSlice.getAuthorInfoAll(value.getAuthorityId());
			for(TimeSliceEntity entity: entities){
				context.write(key, entity);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
}
