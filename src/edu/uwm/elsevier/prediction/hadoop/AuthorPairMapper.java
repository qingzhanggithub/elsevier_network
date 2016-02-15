/**
 * 
 */
package edu.uwm.elsevier.prediction.hadoop;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import pmidmapper.MedlineSearcher;

import edu.uwm.elsevier.indexer.ElsevierCitationIndexer;
import edu.uwm.elsevier.indexer.ElsevierIndexer;
import edu.uwm.elsevier.prediction.AuthorshipEdge;
import edu.uwm.elsevier.prediction.FeatureEntity;
import edu.uwm.elsevier.prediction.RecurringFeatureExtractor;
import edu.uwm.elsevier.prediction.SRWFeatureExtraction;
import edu.uwm.elsevier.prediction.SimpleFeatureExtractor;

/**
 * @author qing
 *
 */
public class AuthorPairMapper extends Mapper<LongWritable, CollabPair, AuthorshipEdge, FeatureEntity> {
	private RecurringFeatureExtractor extractor;
	private static Logger LOGGER = Logger.getLogger("AuthorPairMapper");
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		try {
			extractor =new RecurringFeatureExtractor();
			Path[] cachePathArray = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			ElsevierCitationIndexer.ELSEVIER_CITATION_INDEX_PATH = cachePathArray[0].toString();
			ElsevierIndexer.INDEX_PATH = cachePathArray[1].toString();
			MedlineSearcher.defaultMedlineIndexPath = cachePathArray[2].toString();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	} 
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		try {
			extractor.closeAllStuff();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
		}
	}
	@Override
	protected void map(LongWritable key, CollabPair value,Context context)throws IOException, InterruptedException {
			AuthorshipEdge edge = new AuthorshipEdge(value.getSrc(), value.getDest());
			String cls = context.getConfiguration().get("class_label");
			int year = value.getEndYear();
			if(cls.endsWith("neg"))
				year = 2012;
			try {
				FeatureEntity feature = extractor.getFeatureForPair(edge, year);
				context.write(edge, feature);
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
			} catch (ParseException e) {
				LOGGER.error(e.getMessage());
			}
		
	}
	
	
	
}
