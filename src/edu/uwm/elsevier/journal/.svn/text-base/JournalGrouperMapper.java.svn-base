/**
 * 
 */
package edu.uwm.elsevier.journal;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import articlesdata.article.Article;

/**
 * @author qing
 *
 */
public class JournalGrouperMapper extends
		Mapper<LongWritable, Article, IntWritable, JournalRecord> {
	private JournalNameDsb dsb;
	private Logger LOGGER = Logger.getLogger(JournalGrouperMapper.class);

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		if(dsb !=null){
			try {
				dsb.closeAllStuff();
			} catch (SQLException e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage());
			}
		}
	}

	@Override
	protected void map(LongWritable key, Article value,Context context)
			throws IOException, InterruptedException {
		JournalRecord record = dsb.groupJournalNamesByInitialsFromDB(value);
		if(record !=null){
			context.write(new IntWritable(value.getArticleId()), record);
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try {
			dsb = new JournalNameDsb();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
	}
	
	

}
