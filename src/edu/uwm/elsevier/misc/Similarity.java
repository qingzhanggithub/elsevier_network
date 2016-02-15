/**
 * 
 */
package edu.uwm.elsevier.misc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanQuery;

import edu.uwm.elsevier.ElsevierArticleMetaDataSearcher;
import edu.uwm.elsevier.authoranalysis.InterDisipline;
import pmidmapper.MedlineSearcher;
import pmidmapper.PMArticle;

/**
 * @author qing
 *
 */
public class Similarity {
	
	private MedlineSearcher searcher = null;
	private InterDisipline interDisipline = null;
	public Similarity() throws IOException, ClassNotFoundException, SQLException{
		searcher = new MedlineSearcher(MedlineSearcher.defaultMedlineIndexPath);
		interDisipline = new InterDisipline(2014);
	}
	
	
	public Map<String, Float> getArticles(String queryStr) throws IOException, SQLException, ParseException{
		BooleanQuery query = ElsevierArticleMetaDataSearcher.getQuerySimple(queryStr);
		List<PMArticle> articles = searcher.getPMArticlesByQuery(query);
		List<Long> pmids = new ArrayList<Long>();
		for(PMArticle article: articles){
			pmids.add(article.getPmid());
		}
		Map<String, Float> tfidfs = interDisipline.getTFIDFForPMIDs(pmids);
		return tfidfs;
	}
	
	
	public float getCosineBetweenTopics(String srcTopic, String destTopic) throws IOException, SQLException, ParseException{
		Map<String, Float> srcTfidfs = getArticles(srcTopic);
		Map<String, Float> destTfidfs = getArticles(destTopic);
		float cosine = (float)InterDisipline.getCosine(srcTfidfs, destTfidfs);
		return cosine;
	}
	
	

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, ParseException {
		if(args.length != 3){
			System.out.println("--Topic-A --Topic-B --num-of-return");
			return;
		}
		
		MedlineSearcher.NUM_OF_SEARCH_RESTURN = Integer.parseInt(args[2]);
		Similarity sim = new Similarity();
		float cosine = sim.getCosineBetweenTopics(args[0], args[1]);
		System.out.println("cosin:\t"+cosine);

	}

}
