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

/**
 * @author qing
 *
 */
public class NetworkBuilderMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text word = new Text();
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("NetworkBuilderMap");
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
    public void map(LongWritable k1, Text v1, Context context){
		String line = v1.toString();
		word.set("CID"+line);
		if(line.length() >0){
			long citationId = Long.parseLong(line);
			try {
				networkBuilder.mapCitationToAritcle(citationId);
			} catch (SQLException e) {
				e.printStackTrace();
				LOGGER.error(word.toString()+"\n"+e.getMessage());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				LOGGER.error(word.toString()+"\n"+e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.error(word.toString()+"\n"+e.getMessage());
			} catch (ParseException e) {
				e.printStackTrace();
				LOGGER.error(word.toString()+"\n"+e.getMessage());
			}
		}
	}
}
