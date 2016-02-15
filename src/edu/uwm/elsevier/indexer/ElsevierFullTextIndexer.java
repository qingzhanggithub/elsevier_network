/**
 * 
 */
package edu.uwm.elsevier.indexer;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class ElsevierFullTextIndexer {
	
	public static String FULL_TEXT_INDEX = "/Users/qing/elseiver_fulltext_index";
	public static String TITLE_FIELD = "title";
	public static String ABSTRACT_FIELD = "abstract";
	public static String BODY_FIELD = "body";
	public static String DATE = "date";
	public static String IDENTIFIER = "identifier";
	public static String ARTICLE_ID = "article_id";
	private ArticleService articleService ;
	private AuthorService authorService;
	private ArticlesDataDBConnection databaseConnection;
	private int maxArticleId = 25438518;
	
	private IndexWriter writer ;

	public ElsevierFullTextIndexer() throws IOException, ClassNotFoundException, SQLException{
		NIOFSDirectory directory = new NIOFSDirectory(new File(FULL_TEXT_INDEX));
		IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35));
		writer = new IndexWriter(directory, conf);
		articleService = new ArticleService();
		authorService = new AuthorService();
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	public void indexFullTextFromDB(int start) throws SQLException{
		String sql = "select * from article where source_id=1 ";
		int lower = start;
		int page = 1000;
		int upper = lower+ page;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = null;
		while(lower < maxArticleId){
			rs = stmt.executeQuery(sql+" and article_id>="+lower+" and article_id <"+upper);
			while(rs.next()){
				Article article = articleService.mapResultSetToArticle(rs);
				List<Author> authors = authorService.getAuthorListByArticleId(article.getArticleId());
				
			}
		}
	}
	
	public String getAbstractByArticleId(int articleId) throws SQLException{
		String sql ="select text from sentence, section where section.article_id="+articleId+" and section_name=\'Abstract\' and sentence.section_id=section.section_id";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		StringBuffer sb = new StringBuffer();
		while(rs.next()){
			sb.append(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return sb.toString();
	}
	
	public String getBodyByArticleId(int articleId) throws SQLException{
		String sql ="select text from sentence, section where section.article_id="+articleId+" and section_name!=\'Abstract\' and sentence.section_id=section.section_id";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		StringBuffer sb = new StringBuffer();
		while(rs.next()){
			sb.append(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return sb.toString();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int articleId = 510;
		
		try {
			ElsevierFullTextIndexer indexer = new ElsevierFullTextIndexer();
			String abs = indexer.getAbstractByArticleId(articleId);
			String body = indexer.getBodyByArticleId(articleId);
			System.out.println("ABSTRACT\n"+abs);
			System.out.println("BODY\n"+body);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
	}

}
