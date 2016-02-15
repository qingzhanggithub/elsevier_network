/**
 * 
 */
package edu.uwm.elsevier.grant;

import java.sql.SQLException;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class GrantExtractor {
	
	private ArticlesDataDBConnection databaseConnection;
	
	public GrantExtractor() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	public void extractGrant(){
		
	}
	
	public void extractGrantByArticleId(int articleId){
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
