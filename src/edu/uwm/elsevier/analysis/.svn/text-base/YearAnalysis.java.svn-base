/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.uwm.elsevier.ITableNames;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class YearAnalysis {

	private ArticlesDataDBConnection  databaseConnection;
	private Logger LOGGER = Logger.getLogger(YearAnalysis.class);
	private int maxArticleId = 25435365;
	
	public YearAnalysis() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	
	public void getAgeForMedlineLinks(String save) throws SQLException, IOException{
		FileWriter writer = new FileWriter(save);
		String sql = "select src_article_id, dest_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE;
		int lower = 0;
		int page = 1000;
		int upper = page;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs =null;
		
		while(lower < maxArticleId){
			int counter = 0;
			LOGGER.info("lower= "+ lower);
			rs = stmt.executeQuery(sql+" where src_article_id>="+lower+" and src_article_id<"+upper);
			while(rs.next()){
				int src = rs.getInt("src_article_id");
				int dest = rs.getInt("dest_article_id");
				int diff = getAgeDifferenceBetweenArticles(src, dest);
				if(diff != -1){
//					LOGGER.info(src+ "-->"+ dest+": "+diff);
					writer.append(String.valueOf(diff)).append("\n");
					counter++;
				}
			}
			lower = upper;
			upper += page;
			LOGGER.info(counter+" ages found");
//			if(lower==20000)// for test only!
//				break;
		}
		LOGGER.info("Task done.");
	}
	
	public int getAgeDifferenceBetweenArticles(int src, int dest) throws SQLException{
		String sql = "select year from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" where article_id in ("+ src+", "+dest+")";
		Statement stmt = databaseConnection.getConnection().createStatement();
		
		List<Integer> years= new ArrayList<Integer>();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			int y = rs.getInt("year");
			years.add(y);
		}
		rs.close();
		stmt.close();
		if(years.size()!= 2){
			return -1;
		}else{
			return Math.abs(years.get(0)-years.get(1));
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length !=1){
			System.out.println("--save");
			return;
		}
		try {
			YearAnalysis ya = new YearAnalysis();
			ya.getAgeForMedlineLinks(args[0]);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
