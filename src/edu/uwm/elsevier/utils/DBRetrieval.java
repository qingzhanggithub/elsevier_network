/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.io.FileWriter;
import java.io.IOException;
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
public class DBRetrieval {

	private static String SELECT_CITATION_ID = "select citation_id from citation_detail where is_originally_parsed=1";
	private static String SELECT_ELSEVIER_ARTICLE_ID ="select article_id from article where source_id=1";
	private ArticlesDataDBConnection conn ;
	private String save = "/home/qzhang/citation_ids/";
	private String articleIdSave = "/home/qzhang/elsevier_article_ids/";
	private Logger LOGGER = NetworkBuilderLogger.getLogger("DB Retrieval");
	private static int CITATION_IDS_PER_FILE = 100000;
	private static int ARTICLE_IDS_PER_FILE = 10000;
	public void getCitationId(int start, int page) throws ClassNotFoundException, SQLException, IOException{
		conn = ArticlesDataDBConnection.getInstance();
		String sql = SELECT_CITATION_ID+" limit "+start+", "+page;
		Statement stmt = conn.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		FileWriter writer = new FileWriter(save);
		int count=0;
		while(rs.next()){
			LOGGER.info("count="+count);
			writer.append(String.valueOf(rs.getLong("citation_id"))).append("\n");
			count++;
			if(count %100 == 0)
				LOGGER.info(count+ "have been retrieved.");
		}
		writer.close();
		LOGGER.info("===Task done===");
	}
	
	
	public void writeAllCitationIds() throws ClassNotFoundException, SQLException, IOException{
		LOGGER.info("====Writing citation ids to file===");
		conn = ArticlesDataDBConnection.getInstance();
		Statement stmt = conn.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(SELECT_CITATION_ID);
		FileWriter writer = null;
		long count=0;
		int num = 0;
		while(rs.next()){
			if(count % CITATION_IDS_PER_FILE == 0){
				LOGGER.info(count+ "have been retrieved.");
				if(writer!=null)
					writer.close();
				writer = new FileWriter(save+num+".txt");
				num++;
			}
			writer.append(String.valueOf(rs.getLong("citation_id"))).append("\n");
			count++;
		}
		rs.close();
		writer.close();
		LOGGER.info("===Task done===");
	}
	
	
	public void writeAllElsevierArticleIds() throws ClassNotFoundException, SQLException, IOException{
		LOGGER.info("===Task Start===\nwritting elsevier article id to file");
		conn = ArticlesDataDBConnection.getInstance();
		Statement stmt = conn.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(SELECT_ELSEVIER_ARTICLE_ID);
		FileWriter writer = null;
		long count=0;
		int num = 0;
		while(rs.next()){
			if(count % ARTICLE_IDS_PER_FILE ==0){
				LOGGER.info(count+" have been retrevied.");
				if(writer!=null)
					writer.close();
				writer = new FileWriter(articleIdSave+num+".txt");
				num++;
			}
			writer.append(String.valueOf(rs.getInt("article_id"))).append("\n");
			count++;
		}
		rs.close();
		writer.close();
		LOGGER.info("===Task done===");
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		if(args.length !=2){
//			System.out.println("--start --page");
//			return;
//		}
		DBRetrieval dbr = new DBRetrieval();
		try {
			dbr.writeAllElsevierArticleIds();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
