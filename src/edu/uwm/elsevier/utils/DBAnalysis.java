/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.sql.SQLException;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class DBAnalysis {
	
	private ArticlesDataDBConnection databaseConnection ;
	
	public DBAnalysis() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	public void getAuthorFreq(){
		String sql = "select count(author.author_id) " +
				"from author, article " +
				"where source_id = 1 and author.article_id = article.article_id " +
				"group by concat_ws(',',last_name, first_name)";
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
