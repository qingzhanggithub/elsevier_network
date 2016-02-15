/**
 * 
 */
package edu.uwm.elsevier.indexer;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import edu.uwm.elsevier.NetworkBuilderLogger;

import articlesdata.article.Article;
import articlesdata.article.ArticleDAOJdbcImpl;
import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.AuthorDAOJdbcImpl;
import articlesdata.article.AuthorService;
import articlesdata.citation.CitationParserUtils;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class ElsevierIndexer {

	private IndexWriter writer ;
	public static String TITLE_FIELD = "title";	//used by citation index too.
	public static String AUTHORS_FIELD = "authors";	//used by citation index too.
	public static String JOURNAL_FIELD = "journal";	//used by citation index too.
	public static String ABSTRACT_FIELD = "abstract";
	public static String BODY_FIELD = "body";
	public static String ARTICLE_ID_FIELD = "article_id";
	public static String SOURCE_ID_FIELD ="source_id";
	public static String IDENTIFIER_FIELD = "identifier";
	public static String DATE = "date";
	public static String INDEX_PATH = "/home/qzhang/elsevier_fulltext_index";
	public static String NAME_DELIMITER = ";";
	public static String FIRST_LAST_NAME_DELIMITER = "$";
	private int maxArticleId = 25438518;
	private ArticleService articleService;
	private AuthorService authorService;
	private ArticlesDataDBConnection databaseConnection ;
	private Logger LOGGER = Logger.getLogger(ElsevierIndexer.class);
	private static String INSERT_STATUS = "insert into elsevier_index_status (article_id, status, ddate) values (?,?,?)";
	private static String SELECT_STATUS = "select status from elsevier_index_status where article_id = ";
	private PreparedStatement prepInsertStatus;
	
	public ElsevierIndexer() throws IOException{
		NIOFSDirectory directory = new NIOFSDirectory(new File(INDEX_PATH));
		IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35));
		writer = new IndexWriter(directory, conf);
	}
	
	public void indexArticle(Article article, List<Author> authors) throws CorruptIndexException, IOException, SQLException{
		Document doc = new Document();
		doc.add(new Field(TITLE_FIELD, article.getTitle()==null?"":StringEscapeUtils.unescapeHtml(article.getTitle()), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
		doc.add(new Field(JOURNAL_FIELD, article.getJournal()==null?"":StringEscapeUtils.unescapeHtml(article.getJournal()),Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
		doc.add(new Field(ARTICLE_ID_FIELD,String.valueOf(article.getArticleId()),Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(SOURCE_ID_FIELD,String.valueOf(article.getSourceId()),Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(IDENTIFIER_FIELD,article.getIdentifier()==null?"":article.getIdentifier(),Field.Store.YES, Field.Index.ANALYZED));
		StringBuilder sb = new StringBuilder();
		for(Author a: authors){
			if(a.getFirstName()!=null)
				sb.append(a.getFirstName()).append(" $ ");
			if(a.getLastName()!=null)
				sb.append(a.getLastName()).append("; ");
		}
		String name = sb.toString();
		doc.add(new Field(AUTHORS_FIELD,name,Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
		String abs = getAbstractByArticleId(article.getArticleId());
		String body = getBodyByArticleId(article.getArticleId());
		doc.add(new Field(ABSTRACT_FIELD, abs==null?"":StringEscapeUtils.unescapeHtml(abs), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
		doc.add(new Field(BODY_FIELD, body==null?"":StringEscapeUtils.unescapeHtml(body), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
		 Date date = article.getDatePublished();
		int year = 0;
		if (date !=null)
			year = CitationParserUtils.getYear(date.toString());
		doc.add(new Field(DATE, String.valueOf(year), Field.Store.YES, Field.Index.NOT_ANALYZED));
		writer.addDocument(doc);
	}
	
	public void indexArticlesFromDB(int begin){
		LOGGER.info("====Indexer Started====");
		try {
			articleService = new ArticleService();
			authorService = new AuthorService();
			databaseConnection = ArticlesDataDBConnection.getInstance();
			prepInsertStatus = databaseConnection.getConnection().prepareStatement(INSERT_STATUS);
			int lower =begin;
			int page = 1000;
			int upper = lower +page;
			String sql = "select * from article where source_id=1 ";
			Statement stmt = databaseConnection.getConnection().createStatement();
			int total = 0;
			while(lower < maxArticleId){
				ResultSet rs = stmt.executeQuery(sql+" and article_id>="+lower+" and article_id<"+upper);
				LOGGER.info(total+" docs have been indexed! (current start="+lower+")");
				while(rs.next()){
					Article article = articleService.mapResultSetToArticle(rs);
					if(hasIndexByCheckingStatus(article.getArticleId()))
						continue;// don't index.
					List<Author> authors = authorService.getAuthorListByArticleId(article.getArticleId());
					try {
						indexArticle(article, authors);
						insertStatus(article.getArticleId(), 1);
						total++;
					} catch (CorruptIndexException e) {
						e.printStackTrace();
						break;
					} catch (IOException e) {
						e.printStackTrace();
						break;
					}
				}
				rs.close();
				lower = upper;
				upper += page;
				writer.commit();
			}
			writer.close();
			databaseConnection.close();
			LOGGER.info("===Task Done====");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void insertStatus(int articleId, int status) throws SQLException{
		prepInsertStatus.setInt(1, articleId);
		prepInsertStatus.setInt(2, status); //1, success; 0- failed.
		prepInsertStatus.setDate(3, new java.sql.Date(Calendar.getInstance().getTimeInMillis()));
		prepInsertStatus.executeUpdate();
	}
	
	private boolean hasIndexByCheckingStatus(int articleId) throws SQLException{
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(SELECT_STATUS+articleId);
		boolean hasIndex = rs.next();
		rs.close();
		return hasIndex;
		
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
		ElsevierIndexer eindexer;
		if(args.length ==0){
			System.out.println("Elsevier Indexer\n--start");
		}
		try {
			eindexer = new ElsevierIndexer();
			eindexer.indexArticlesFromDB(Integer.parseInt(args[0]));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
