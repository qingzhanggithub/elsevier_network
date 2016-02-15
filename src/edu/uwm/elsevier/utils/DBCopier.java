/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import edu.uwm.elsevier.NetworkBuilderLogger;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class DBCopier {

	private ArticlesDataDBConnection databaseConn ;
	private static String INSERT = "insert into cnetworkv4_enhanced" +
			" (citation_id, article_id, title_comp, authors_comp, journal_comp, is_matched) " +
			"values (?, ?, ?, ?, ?, ?)";
	private PreparedStatement prepInsert;
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("AuthorDSB");
	
	public DBCopier() throws ClassNotFoundException, SQLException{
		databaseConn  = ArticlesDataDBConnection.getInstance();
		prepInsert = databaseConn.getConnection().prepareStatement(INSERT);
	}
	
	public void copyTable() throws SQLException{
		String sql = "select * from cnetworkv4 ";
		int lower = 0;
		int page = 5000;
		int upper = lower +page;
		boolean hasMore = true;
		Statement stmt = databaseConn.getConnection().createStatement();
		while(hasMore){
			LOGGER.info("lower="+ lower);
			String fullSql = sql + "where citation_id >= "+lower+" and citation_id < "+upper;
			ResultSet rs = stmt.executeQuery(fullSql);
			hasMore = false;
			while(rs.next()){
				hasMore = true;
				int articleId = rs.getInt("article_id");
				prepInsert.setLong(1, rs.getLong("citation_id"));
				prepInsert.setInt(2, articleId==0?1: articleId);
				prepInsert.setInt(3, rs.getInt("title_comp"));
				prepInsert.setInt(4, rs.getInt("authors_comp"));
				prepInsert.setInt(5, rs.getInt("journal_comp"));
				prepInsert.setInt(6, rs.getInt("is_matched"));
				prepInsert.addBatch();
			}
			prepInsert.executeBatch();
			lower = upper;
			upper += page;
		}
		LOGGER.info("Copy finished.");
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DBCopier copier;
		try {
			copier = new DBCopier();
			copier.copyTable();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
