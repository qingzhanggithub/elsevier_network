/**
 * 
 */
package edu.uwm.elsevier;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import articlesdata.article.Citation;

/**
 * @author qing
 *
 */
public class CitationMergerMap extends Mapper<LongWritable, Citation, LongWritable, MappingStatus>{
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("CitationMergerMap");
	private String indexPath;
	private CitationMerger citationMerger;
	
	@Override
    protected void setup(Context context){
		try {
			Path[] cachePathArray = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			indexPath = cachePathArray[0].toString();
			citationMerger = new CitationMerger(indexPath);
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
    }
	
	@Override
	protected void cleanup(Context context){
		if(citationMerger != null){
			try {
				citationMerger.closeAllStuff();
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	@Override
    public void map(LongWritable k1, Citation v1, Context context){
		try {
			LOGGER.info("In map. v.citationId ="+v1.getCitationId());
//			citationMerger.margeCitation(v1);
			List<CitationCitationMappingStatus> statusList = citationMerger.mergeCitationWithoutDB(v1);
			for(CitationCitationMappingStatus status: statusList){
				context.write(new LongWritable(v1.getArticleId()), status);
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(v1.getCitationId()+"\n"+e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.error(v1.getCitationId()+"\n"+e.getMessage());
		}
	}
}
