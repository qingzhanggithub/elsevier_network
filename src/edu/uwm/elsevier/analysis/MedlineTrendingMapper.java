/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.NetworkBuilderLogger;

import pmidmapper.PMArticle;

/**
 * @author qing
 *
 */
public class MedlineTrendingMapper extends
		Mapper<LongWritable, PMArticle, IntWritable, MEDLINEArticleTrendingEntity> {
	private MedlineNetworkAnalysis analysis;
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("MedlineTrendingMapper");

	@Override
    protected void setup(Context context){
		try {
			analysis = new MedlineNetworkAnalysis();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
	}
	
	
	
	@Override
    public void map(LongWritable k1, PMArticle v1, Context context){
		try {
			MEDLINEArticleTrendingEntity entity = analysis.getCiationYearTrending(v1);
			context.write(new IntWritable(v1.getArticleId()), entity);
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error("articleId:"+v1.getArticleId()+"\n"+e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error("articleId:"+v1.getArticleId()+"\n"+e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("articleId:"+v1.getArticleId()+"\n"+e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.error("articleId:"+v1.getArticleId()+"\n"+e.getMessage());
		}
	}
	
}
