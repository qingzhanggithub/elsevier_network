/**
 * 
 */
package edu.uwm.elsevier;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import articlesdata.article.Article;
import articlesdata.article.AuthorDAOJdbcImpl;
import articlesdata.article.AuthorService;
import articlesdata.article.Citation;
import articlesdata.citation.Author;
import articlesdata.citation.CitationDAOJdbcImpl;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class CitationNetworkBuilder {

	private static Logger LOGGER = NetworkBuilderLogger.getLogger("CitationNetworkBuilder");
	private static String INSERT_MATCHED = "insert into cnetworkv4 " +
			"(citation_id, article_id, title_comp, authors_comp, journal_comp, is_matched, ddate)" +
			"value (?,?,?,?,?,?,?)";
	private static String SELECT_NETWORK_BUILD_STATUS = "select citation_id from cnetworkv3_status where citation_id= ?";
	private static String INSERT_NETWORK_BUILD_STATUS ="insert into cnetworkv3_status (citation_id, ddate) values (?, ?)"; 
	private static String SELECT_NETWORK_BUILD_STATUS_V4 = "select citation_id from cnetworkv4 where citation_id= ?";
	
	
//	private ArticlesDataDBConnection dataBaseConnection;
	private Connection conn;
	private CitationDAOJdbcImpl citationDaoJdbcImpl;
//	private AuthorDAOJdbcImpl authorDaoJdbcImple;
	private PreparedStatement prepInsertMatched;
	private PreparedStatement prepInsertBuildStatus;
	private PreparedStatement prepSelectBuildStatus;
	private PreparedStatement prepSelectMappingStatusV4;
	private ElsevierArticleMetaDataSearcher searcher;
	
	private static CitationNetworkBuilder singleton  = null;
	
	
	public static CitationNetworkBuilder getInstance(String indexPath) throws ClassNotFoundException, SQLException, IOException{
		if(singleton == null){
			singleton = new CitationNetworkBuilder(indexPath);
		}
		return singleton;
	}
	
	public CitationNetworkBuilder(String indexPath) throws ClassNotFoundException, SQLException, IOException{
		conn = ArticlesDataDBConnection.getInstance().getConnection();
//		citationDaoJdbcImpl = new CitationDAOJdbcImpl();
//		authorDaoJdbcImple = new AuthorDAOJdbcImpl();
		prepInsertMatched = conn.prepareStatement(INSERT_MATCHED);
		prepInsertBuildStatus = conn.prepareStatement(INSERT_NETWORK_BUILD_STATUS);
		prepSelectBuildStatus = conn.prepareStatement(SELECT_NETWORK_BUILD_STATUS);
		prepSelectMappingStatusV4 = conn.prepareStatement(SELECT_NETWORK_BUILD_STATUS_V4);
		searcher = new ElsevierArticleMetaDataSearcher(indexPath);
	}
	
	public void mapCitationToAritcle(long citationId) throws SQLException, IOException, ClassNotFoundException, ParseException{
		if(hasCitationChecked(citationId))
			return;	//citation has been check in previous process.
		LOGGER.info("Checking citaiton "+citationId+" with articles");
		insertBuildStatus(citationId);	//otherwise insert into status to indicate it will be checked in this run.
		Citation citation = citationDaoJdbcImpl.getCitationByCitationId(citationId);// has authors in it.
		List<Author> authorsC = citation.getAuthors();
		List<Article> articles = searcher.getArticleIds(citation.getTitle(), authorsC);	// authors of the articles are retrieved too!!
		
//		List<articlesdata.article.Author> authorList = authorDaoJdbcImple.getAuthorListByArticleIdList(articleIds);
		Map<Integer, List<articlesdata.article.Author>> articleIdAuthorMap = searcher.getAuthorMap();
		
		for(Article article: articles){
//			List<articlesdata.article.Author> authorsA = authorDaoJdbcImple.getAuthorListByArticleId(article.getArticleId());
			List<articlesdata.article.Author> authorsA = articleIdAuthorMap.get(article.getArticleId());
			
			MappingStatus status = new MappingStatus(citationId, article.getArticleId());
			CitationArticleComparison.isCitationAndArticleEqual(citation, article, authorsA, status);//!!!UPDATE STATUS
			if(CitationArticleComparison.isEqual(status)){
//				status.setDate(Calendar.getInstance().getTime());
				status.setIsMatched(1);
				insertMappingStatus(status);	// just insert ones that have something matched. ignore the IRRELEVANT ones.
				return;
			}
		}
	}
	
	
	public void mapCitationToAritcle(Citation citation) throws ParseException, IOException, ClassNotFoundException, SQLException{
		if(hasCitationProcessed(citation.getCitationId()))
			return;
		List<Article> articles = searcher.getArticleIds(citation.getTitle(), citation.getAuthors());
		Map<Integer, List<articlesdata.article.Author>> articleIdAuthorMap = searcher.getAuthorMap();
		MappingStatus status = new MappingStatus(citation.getCitationId());	// if no article to compare, this one will be returned. its article field is null.
		MappingStatus topArticle = null;
		boolean hasTopProcessed = false;
		for(Article article: articles){
			status = new MappingStatus(citation.getCitationId(), article.getArticleId(),0); // default is_matched = 0
			List<articlesdata.article.Author> authorsA = articleIdAuthorMap.get(article.getArticleId());
			CitationArticleComparison.isCitationAndArticleEqual(citation, article, authorsA, status);//!!!UPDATE STATUS
			if(status.isEqual()){
				status.setIsMatched(1);	// isMatched = 1 for the matched pair. -1 by default
				break;
			}
			if(!hasTopProcessed){
				topArticle = status;
				hasTopProcessed = true;
			}
		}
		if(status.getIsMatched() == 0)	// if none of the articles are matched to the citation, return the top one
			insertMappingStatus(topArticle);
		else
			insertMappingStatus(status);
			
	}
	
	public void mapRawCitationToArticle(RawCitation citation) throws SQLException, ParseException, IOException, ClassNotFoundException{
		if(hasCitationProcessed(citation.getCitationId()))
			return;
		List<Article> articles = searcher.getArticleIdsSimple(citation.getCitationText());
		Map<Integer, List<articlesdata.article.Author>> articleIdAuthorMap = searcher.getAuthorMap();
		MappingStatus status = new MappingStatus(citation.getCitationId());	// if no article to compare, this one will be returned. its article field is null.
		MappingStatus topArticle = null;
		boolean hasTopProcessed = false;
		for(Article article: articles){
			status = new MappingStatus(citation.getCitationId(), article.getArticleId(),0); // default is_matched = 0
			List<articlesdata.article.Author> authorsA = articleIdAuthorMap.get(article.getArticleId());
			BibCompareSimple.isRawCitationTextAndArticleEqual(citation.getCitationText(), article, authorsA, status);
			if(status.isEqual()){
				status.setIsMatched(1);	// isMatched = 1 for the matched pair. -1 by default
				break;
			}
			if(!hasTopProcessed){
				topArticle = status;
				hasTopProcessed = true;
			}
		}
		if(status.getIsMatched() == 0)	// if none of the articles are matched to the citation, return the top one
			insertMappingStatus(topArticle);
		else
			insertMappingStatus(status);
	}
	
	private void insertMappingStatus(MappingStatus status) throws SQLException{
		LOGGER.info("Inserting mapping status. citation_id="+status.getCitationId()+"\tisMatched="+status.getIsMatched());
		prepInsertMatched.setLong(1, status.getCitationId());
		prepInsertMatched.setInt(2, status.getArticleId());
		prepInsertMatched.setInt(3, status.getTitleComp());
		prepInsertMatched.setInt(4, status.getAuthorsComp());
		prepInsertMatched.setInt(5, status.getJournalComp());
		prepInsertMatched.setInt(6, status.getIsMatched());
		prepInsertMatched.setDate(7, new java.sql.Date(Calendar.getInstance().getTimeInMillis()));
		prepInsertMatched.executeUpdate();
	}
	
	private void insertBuildStatus(long citationId) throws SQLException{
		prepInsertBuildStatus.setLong(1, citationId);
		prepInsertBuildStatus.setDate(2, new java.sql.Date(Calendar.getInstance().getTimeInMillis()));
		prepInsertBuildStatus.executeUpdate();
	}
	
	private boolean hasCitationChecked(long citationId) throws SQLException{
		prepSelectBuildStatus.setLong(1, citationId);
		ResultSet rs = prepSelectBuildStatus.executeQuery();
		boolean checked = rs.next();
		rs.close();
		return checked;
	}
	
	private boolean hasCitationProcessed(long citationId) throws SQLException{
		prepSelectMappingStatusV4.setLong(1, citationId);
		ResultSet rs = prepSelectMappingStatusV4.executeQuery();
		boolean checked = rs.next();
		rs.close();
		return checked;
	}
	
	public void closeAllStuff() throws SQLException, IOException{
		if(prepInsertMatched !=null)
			prepInsertMatched.close();
		if(conn !=null)
			conn.close();
		if(searcher !=null)
			searcher.close();
		LOGGER.info("DB Connection and index searcher have been closed.");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		if(args.length !=1){
//			System.out.println("CitationNetworkBuilder\n--citation-id");
//			return;
//		}
//		try {
//			CitationNetworkBuilder  cnb = new CitationNetworkBuilder();
//			cnb.mapCitationToAritcle(1089881);
//			cnb.closeAllStuff();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//			LOGGER.error(e.getStackTrace());
//		} catch (SQLException e) {
//			e.printStackTrace();
//			LOGGER.error(e.getStackTrace());
//		} catch (NumberFormatException e) {
//			e.printStackTrace();
//			LOGGER.error(e.getStackTrace());
//		} catch (IOException e) {
//			e.printStackTrace();
//			LOGGER.error(e.getStackTrace());
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
	}

}
