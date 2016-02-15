package edu.uwm.elsevier.analysis;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import pmidmapper.PMArticle;

import edu.uwm.elsevier.NetworkBuilderLogger;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

public class MedlineNetworkAnalyzerMapper extends Mapper<LongWritable, PMArticle, LongWritable, MedlineAritcleNodeStatistiscs> {
	private MedlineNetworkBasicStatistcsExtractor analysis;
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("MedlineNetworkAnalyzerMapper");
	private String indexPath;
	
	@Override
    protected void setup(Context context){
		try {
			
			Path[] cachePathArray = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			indexPath = cachePathArray[0].toString();
			analysis = new MedlineNetworkBasicStatistcsExtractor(indexPath);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
	}
	@Override
    public void map(LongWritable k1, PMArticle v1, Context context){
		try {
			MedlineAritcleNodeStatistiscs stats = analysis.extractStatistics(v1.getArticleId(), v1.getPmid());
//			analysis.extractStatistics(v1.getArticleId(), v1.getPmid());
			context.write(new LongWritable(stats.getArticleId()), stats);
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error("article"+v1.getArticleId()+"\n"+e.getMessage());
		} catch (ParseException e) {
			e.printStackTrace();
			LOGGER.error("article"+v1.getArticleId()+"\n"+e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("article"+v1.getArticleId()+"\n"+e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
