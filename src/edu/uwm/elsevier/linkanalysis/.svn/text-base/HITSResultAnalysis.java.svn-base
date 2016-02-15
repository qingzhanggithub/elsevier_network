/**
 * 
 */
package edu.uwm.elsevier.linkanalysis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class HITSResultAnalysis {

	
	private ArticlesDataDBConnection databaseConnection ;
	private AuthorService authorService;
	private ArticleService articleService;
	
	public HITSResultAnalysis() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	public void processResult(String rankingPath, String type){
		
	}
	
	public void getArticle(int index) throws SQLException{
		String sql = "select article_id from medline_matrix_src_by_dest where matrix_index="+index;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int articleId = -1;
		if(rs.next()){
			articleId = rs.getInt(1);
		}
		Article article = articleService.getArticleByArticleId(articleId);
		List<Author> authors = authorService.getAuthorListByArticleId(articleId);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
