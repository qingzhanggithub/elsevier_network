/**
 * 
 */
package edu.uwm.elsevier.medline;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;

import articlesdata.database.ArticlesDataDBConnection;
import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.NetworkBuilderLogger;

/**
 * @author qing
 *
 */
public class MedlineCitationNetwork {

	private ArticlesDataDBConnection databaseConnection ;
	private CitationNetworkService citationNetworkService;
	private String insertMedlineEdge = "insert into medline_network_v2 (src_article_id, dest_article_id) " +
			"values (?, ?) ON DUPLICATE KEY UPDATE src_article_id=src_article_id";
	private PreparedStatement prepInsertMedlineEdge;
	private int maxArticleId = 25438518;
	private Logger LOGGER = NetworkBuilderLogger.getLogger("MedlineCitationNetwork");
	
	public MedlineCitationNetwork() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
		citationNetworkService = new CitationNetworkService();
		prepInsertMedlineEdge = databaseConnection.getConnection().prepareStatement(insertMedlineEdge);
	}
	public void generateMedlineCitationNetworkFromElsevier() throws SQLException{
		LOGGER.info("Start generating medline network.");
		String sql = "select distinct article_id from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE+" where is_matched=1 ";
		int lower = 0;
		int	page = 1000;
		int upper = lower+page;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = null;
		while(lower < maxArticleId){
			LOGGER.info("lower="+lower);
			rs = stmt.executeQuery(sql+" and article_id >="+lower+" and article_id<"+upper);
			while(rs.next()){
				int articleId = rs.getInt(1);
				List<Integer> incites = citationNetworkService.getInciteMedlineArticleIdsForArticleId(articleId);
				LOGGER.info(incites.size()+" incites for article "+articleId);
				for(int incite : incites){
					prepInsertMedlineEdge.setInt(1, incite);
					prepInsertMedlineEdge.setInt(2, articleId);
					prepInsertMedlineEdge.addBatch();
//					LOGGER.info("("+incite+", "+articleId+") found.");
				}
				prepInsertMedlineEdge.executeBatch();
			}
			
			lower= upper;
			upper += page;
		}
		rs.close();
		stmt.close();
		LOGGER.info("Task done.");
	}
	
	
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		MedlineCitationNetwork mnetwork = new MedlineCitationNetwork();
		mnetwork.generateMedlineCitationNetworkFromElsevier();
	}

}
