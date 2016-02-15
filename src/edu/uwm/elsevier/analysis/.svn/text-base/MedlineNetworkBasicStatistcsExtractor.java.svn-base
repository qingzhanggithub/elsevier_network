/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import pmidmapper.MedlineSearcher;
import pmidmapper.PMArticle;

import articlesdata.article.ArticleService;
import articlesdata.article.AuthorService;
import articlesdata.citation.CitationParserUtils;
import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.NetworkBuilderLogger;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

/**
 * @author qing
 *
 */
public class MedlineNetworkBasicStatistcsExtractor {
	private MedlineSearcher medlineSearcher;
	private ArticlesDataDBConnection databaseConnection;
	private PreparedStatement prepInsertStats;
	private int maxArticleId = 25438518;
	private static String INSERT_STATS = "insert into medline_network_statistics " +
			"(article_id, pmid, incite, incite_from_medlline, outcite, outcite_to_medline, num_of_authors, year, journal, meshs)" +
			"values (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE article_id=article_id";
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("MedlineNetworkAnalysis");
	public MedlineNetworkBasicStatistcsExtractor(String indexPath) throws ClassNotFoundException, SQLException, IOException{
		medlineSearcher = new MedlineSearcher(indexPath);
		databaseConnection = ArticlesDataDBConnection.getInstance();
		prepInsertStats = databaseConnection.getConnection().prepareStatement(INSERT_STATS);
		
	}

	public void generateMedlineCitationNetworkStatisticsTable() throws SQLException, ParseException, IOException{
		LOGGER.info("Start generating stats..");
		String sql = "select article_id, pmid from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE+" where is_matched=1 ";
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = null;
		int lower = 0;
		int page = 1000;
		int upper = lower+page;
		int index =0;
		
		while(lower< maxArticleId){
			LOGGER.info("lower="+lower);
			rs =stmt.executeQuery(sql+" and article_id>="+lower+" and article_id<"+upper);
			while(rs.next()){
				int articleId = rs.getInt("article_id");
				long pmid = rs.getLong("pmid");
				MedlineAritcleNodeStatistiscs stats = extractStatistics(articleId, pmid);
				insertStats(stats);
			}
			prepInsertStats.executeBatch();
			lower = upper;
			upper += page;
		}
		rs.close();
		stmt.close();
		prepInsertStats.close();
		LOGGER.info("Task done. "+index+" have been processed.");
	}
	
	public MedlineAritcleNodeStatistiscs extractStatistics(int articleId, long pmid) throws SQLException, ParseException, IOException{
		MedlineAritcleNodeStatistiscs stats = new MedlineAritcleNodeStatistiscs(articleId, pmid);
		int incite = getInciteCount(articleId);
		int inciteFromMedline = getInciteFromMedlineCount(articleId);
		int outcite = getOutciteCountByArticleId(articleId);
		int outciteToMedline = getOutciteToMedlineCount(articleId);
		int numOfAuthors = getAuthorCount(articleId);
		PMArticle pmArticle = medlineSearcher.getPMArticleByPMID(pmid, false);
		String year = pmArticle.getYear();
		String journal = pmArticle.getJournal();
		if(journal==null || journal.length()==0)
			journal ="NA";
		String meshs = pmArticle.getMeshs();
		if(meshs==null || meshs.length() ==0)
			meshs ="NA";
		stats.setIncites(incite);
		stats.setIncitesFromMedline(inciteFromMedline);
		stats.setOutcites(outcite);
		stats.setOutcitesToMedline(outciteToMedline);
		stats.setNumOfAuthors(numOfAuthors);
		stats.setJournal(journal);
		if(year !=null)
			stats.setYear(CitationParserUtils.getYear(year));
		stats.setMeshs(meshs);
//		insertStats(stats);
		return stats;
	}
	
	private int getAuthorCount(int articleId) throws SQLException{
		String sql = "select count(author_id) from "+ITableNames.AUTHOR_TABLE+" where article_id="+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int count=0;
		if(rs.next()){
			count=rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return count;
		
	}
	
	private int getOutciteCountByArticleId(int articleId) throws SQLException{
		String sql = "select count(citation_id) from "+ITableNames.CITATION_TABLE+" where article_id="+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int count=0;
		if(rs.next()){
			count=rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return count;
	}
	
	private int getInciteCount(int articleId) throws SQLException{
		String sql = "select count(citation_id) from " +
				ITableNames.CITATION_MAPPING_TABLE+
				" where article_id ="+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int count =0;
		if(rs.next()){
			count=rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return count;
	}
	
	private int getInciteFromMedlineCount(int articleId) throws SQLException{
		String sql = "select count(src_article_id) from "+ITableNames.MEDLINE_NETWORK_TABLE+" where dest_article_id="+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int count=0;
		if(rs.next()){
			count =(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return count;
	}
	
	private int getOutciteToMedlineCount(int articleId) throws SQLException{
		String sql = "select count(dest_article_id) from "+ITableNames.MEDLINE_NETWORK_TABLE+" where src_article_id= "+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int count=0;
		if(rs.next()){
			count = (rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return count;
	}
	
	public void insertStats(MedlineAritcleNodeStatistiscs stats) throws SQLException{
		LOGGER.info("Inserting stats article"+stats.getArticleId());
		prepInsertStats.setInt(1, stats.getArticleId());
		prepInsertStats.setLong(2, stats.getPmid());
		prepInsertStats.setInt(3, stats.getIncites());
		prepInsertStats.setInt(4, stats.getIncitesFromMedline());
		prepInsertStats.setInt(5, stats.getOutcites());
		prepInsertStats.setInt(6, stats.getOutcitesToMedline());
		prepInsertStats.setInt(7, stats.getNumOfAuthors());
		prepInsertStats.setInt(8, stats.getYear());
		prepInsertStats.setString(9, stats.getJournal());
		prepInsertStats.setString(10, stats.getMeshs());
//		prepInsertStats.addBatch();
		prepInsertStats.executeUpdate();
	}
	
	public void closeAllStuff() throws SQLException, IOException{
		if(databaseConnection !=null){
			databaseConnection.close();
		}
		if(medlineSearcher !=null){
			medlineSearcher.close();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			MedlineNetworkBasicStatistcsExtractor analysis = new MedlineNetworkBasicStatistcsExtractor(MedlineSearcher.defaultMedlineIndexPath);
			analysis.generateMedlineCitationNetworkStatisticsTable();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}

}
