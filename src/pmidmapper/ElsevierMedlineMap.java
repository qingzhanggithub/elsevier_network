/**
 * 
 */
package pmidmapper;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import articlesdata.article.Article;
import edu.uwm.elsevier.ElsevierMedlineMappingStatus;
import edu.uwm.elsevier.NetworkBuilderLogger;

/**
 * @author qing
 *
 */
public class ElsevierMedlineMap extends Mapper<LongWritable, Article, IntWritable, ElsevierMedlineMappingStatus>{
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("ElsevierMedlineMap");
	private String indexPath;
	private ElsevierMedlineMapper elsMedline;
	
	@Override
    protected void setup(Context context){
		try {
			Path[] cachePathArray = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			indexPath = cachePathArray[0].toString();
			elsMedline = new ElsevierMedlineMapper(indexPath);
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
		if(elsMedline !=null){
			try {
				elsMedline.closeAllStuff();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	@Override
    public void map(LongWritable k1, Article v1, Context context){
		try {
//			LOGGER.info("reading from db -article_id:"+v1.getArticleId());
			ElsevierMedlineMappingStatus status = elsMedline.mapElsevierToMedline(v1);
			if(status !=null){
//				LOGGER.info("put article"+status.getArticleId()+" to reducer");
				context.write(new IntWritable(v1.getArticleId()), status);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
