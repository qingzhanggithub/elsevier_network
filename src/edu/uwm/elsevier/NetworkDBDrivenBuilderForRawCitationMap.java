/**
 * 
 */
package edu.uwm.elsevier;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import articlesdata.citation.CitationParserUtils;

/**
 * @author qing
 *
 */
public class NetworkDBDrivenBuilderForRawCitationMap extends Mapper<LongWritable, RawCitation, Text, IntWritable>{
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("NetworkDBDrivenBuilderForRawCitationMap");
	private String indexPath;
	private CitationNetworkBuilder networkBuilder;
	
	@Override
    protected void setup(Context context){
		try {
			Path[] cachePathArray = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			indexPath = cachePathArray[0].toString();
			networkBuilder = CitationNetworkBuilder.getInstance(indexPath);
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
	
	@Override
	protected void cleanup(Context context){
		if(networkBuilder != null){
			try {
				networkBuilder.closeAllStuff();
			} catch (SQLException e) {
				LOGGER.info(e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				LOGGER.info(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	@Override
    public void map(LongWritable k1, RawCitation v1, Context context){
		try {
			networkBuilder.mapRawCitationToArticle(v1);
		} catch (ParseException e) {
			e.printStackTrace();
			LOGGER.error(v1.getCitationId()+"\n"+e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(v1.getCitationId()+"\n"+e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error(v1.getCitationId()+"\n"+e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(v1.getCitationId()+"\n"+e.getMessage());
		}
	}

}
