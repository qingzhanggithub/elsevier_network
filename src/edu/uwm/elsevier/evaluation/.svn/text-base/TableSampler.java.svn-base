/**
 * 
 */
package edu.uwm.elsevier.evaluation;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import articlesdata.article.ArticleService;
import articlesdata.article.Citation;
import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.ITableNames;

/**
 * @author qing
 *
 */
public class TableSampler {

	private static List<Long> commonCitationIds;
	private static List<Integer> commomArticleIds;
	private static Logger LOGGER = SamplingLogger.getLogger("TableSampler");
	
	public TableSampler() throws ClassNotFoundException, SQLException{
	}
	
	public static List<Long> getCommomCitationIds(int size, int max) throws SQLException, ClassNotFoundException{
		if(commonCitationIds == null)
			commonCitationIds = sampleCitationTable(size, max);
		return commonCitationIds;
	}
	
	public static List<Integer> getCommonArticleIds(int size, int max) throws SQLException, ClassNotFoundException{
		if(commomArticleIds == null){
			commomArticleIds = sampleArticleTable(size, max);
		}
		return commomArticleIds;
	}
	
	public static List<Long> sampleCitationTable(int size, int max) throws SQLException, ClassNotFoundException{
		LOGGER.info("Sampling citation table size="+size+", max="+max);
		HashSet<Integer> seeds = new HashSet<Integer>();
		Random r = new Random();
//		String sql = " select citation_id from "+ITableNames.CITATION_DETAIL_TABLE+" where is_originally_parsed=1 limit ?, 1";
		String sql = " select citation_id from "+ITableNames.MERGED_CITATION_TABLE+" limit ?, 1";
		PreparedStatement prepSelect = ArticlesDataDBConnection.getInstance().getConnection().prepareStatement(sql);
		ResultSet rs = null;
		List<Long> citationIds = new ArrayList<Long>();
		int seed = -1;
		int index = 0;
		while(seeds.size()< size){
			seed = r.nextInt(max)+1;
			if(seeds.contains(seed))
				continue;
			prepSelect.setInt(1, seed);
			rs = prepSelect.executeQuery();
			if(rs.next()){
				citationIds.add(rs.getLong(1));
				seeds.add(seed);
				index++;
				LOGGER.info(index+"/"+size+": citation id ="+seed);
			}
		}
		rs.close();
		prepSelect.close();
		LOGGER.info("Finished sampling citation table.");
		return citationIds;
	}
	
	public static List<Integer> sampleArticleTable(int size, int max) throws SQLException, ClassNotFoundException{
		LOGGER.info("Sampling article table, size="+size+", max="+max);
		HashSet<Integer> seeds = new HashSet<Integer>();
		Random r = new Random();
		String sql = "select article_id from elsevier_pmid_mapping where is_matched=1 limit ?, 1";
		PreparedStatement prepSelect = ArticlesDataDBConnection.getInstance().getConnection().prepareStatement(sql);
		ResultSet rs = null;
		List<Integer> articleIds = new ArrayList<Integer>();
		int seed = -1;
		int index =0;
		while(seeds.size()< size){
			seed = r.nextInt(max)+1;
			if(seeds.contains(seed))
				continue;
			prepSelect.setInt(1, seed);
			rs = prepSelect.executeQuery();
			if(rs.next()){
				articleIds.add(rs.getInt(1));
				seeds.add(seed);
				index++;
				LOGGER.info(index+"/"+size+":article id="+seed);
			}
		}
		rs.close();
		prepSelect.close();
		LOGGER.info("Finished sampling article table.");
		return articleIds;
	}
	
	public static long sampleCitationFromArticle(int articleId) throws ClassNotFoundException, SQLException{
		ArticleService articleService = new ArticleService();
		List<Citation> citations = articleService.getCitationListByArticleId(articleId);
		int max = citations.size();
		HashSet<Integer> seeds = Sampler.getSeeds(1, max);
		for(Integer seed: seeds){
			return citations.get(seed).getCitationId();
		}
		return -1;
	}
	
	public static List<Long> samplePMArticleCustomized(int size, int max) throws SQLException, ClassNotFoundException{
		List<Integer> articles = sampleArticleTable(size, max);
		List<Long> citationIds = new ArrayList<Long>();
		for(int articleId: articles){
			citationIds.add(sampleCitationFromArticle(articleId));
		}
		return citationIds;
	}
	
	public static String getInClause(HashSet<Integer> seeds){
		StringBuffer sb = new StringBuffer();
		boolean isFirst = true;
		sb.append("(");
		for(int seed: seeds){
			if(isFirst){
				sb.append(seed);
				isFirst = false;
			}else{
				sb.append(",").append(seed);
			}
		}
		sb.append(")");
		return sb.toString();
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
