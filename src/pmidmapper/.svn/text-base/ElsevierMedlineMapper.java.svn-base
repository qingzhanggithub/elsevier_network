/**
 * 
 */
package pmidmapper;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.search.BooleanQuery;

import edu.uwm.elsevier.CitationArticleComparison;
import edu.uwm.elsevier.ElsevierArticleMetaDataSearcher;
import edu.uwm.elsevier.ElsevierMedlineMappingStatus;
import edu.uwm.elsevier.MappingStatus;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class ElsevierMedlineMapper {

	
	private ArticleService articleService;
	private AuthorService authorService;
	private MedlineSearcher searcher;
	private static String SELECT_ARTICLE_LIMIT = "select * from article where source_id = 1 ";
	private static String INSERT_MAPPING = "insert into elsevier_pmid_mapping (article_id, pmid, title_comp, authors_comp, journal_comp, is_matched, ddate) value (?,?,?,?,?,?,?)";
	private static String SELECT_MAPPING = "select article_id from elsevier_pmid_mapping where article_id = ";
	private ArticlesDataDBConnection conn;
	private PreparedStatement prepInsertMapping;
	private Logger LOGGER = MapperLogger.getLogger("ElsevierMedlineMapper");
	
	public ElsevierMedlineMapper() throws ClassNotFoundException, SQLException, IOException{
		conn = ArticlesDataDBConnection.getInstance();
//		articleService = new ArticleService();
		authorService = new AuthorService();
		prepInsertMapping = conn.getConnection().prepareStatement(INSERT_MAPPING);
		searcher = new MedlineSearcher(MedlineSearcher.defaultMedlineIndexPath);
	}
	
	public ElsevierMedlineMapper(String medlineIndex) throws ClassNotFoundException, SQLException, IOException{
		conn = ArticlesDataDBConnection.getInstance();
//		articleService = new ArticleService();
		authorService = new AuthorService();
		prepInsertMapping = conn.getConnection().prepareStatement(INSERT_MAPPING);
		searcher = new MedlineSearcher(medlineIndex);
	}
	
	public ElsevierMedlineMappingStatus mapElsevierToMedline(Article article) throws SQLException, IOException{
//		LOGGER.info("matching article "+article.getArticleId());
		List<Author> authors = authorService.getAuthorListByArticleId(article.getArticleId());
		BooleanQuery query = ElsevierArticleMetaDataSearcher.getQueryFromAritcle(article.getTitle(), authors);
		List<PMArticle> pmArticles = searcher.getPMArticlesByQuery(query);
		ElsevierMedlineMappingStatus status = null;
		for(PMArticle pma: pmArticles){
			status = new ElsevierMedlineMappingStatus(pma.getPmid(), article.getArticleId());
			ElsevierPMComparison.isElsevierAndPMArticleEqual(article, authors, pma, status);
			if(status.getIsMatched()==1){
//				LOGGER.info("Matched!"+status.toString());
				return status;
			}
		}
		return null;
	}
	
	public void mapElsevierToMedline(int begin) throws SQLException, ClassNotFoundException, IOException{
		LOGGER.info("===Task Start===");
		articleService = new ArticleService();
//		authorService = new AuthorService();
//		prepInsertMapping = conn.getConnection().prepareStatement(INSERT_MAPPING);
//		searcher = new MedlineSearcher(MedlineSearcher.defaultMedlineIndexPath);
		Statement stmt = conn.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		int start = 0;
		int page = 1000;
		boolean hasMore = true;
		ResultSet rs = null;
		while(hasMore){
			LOGGER.info("Current start = "+start);
			rs = stmt.executeQuery(SELECT_ARTICLE_LIMIT+"  and article_id >"+begin+" limit "+start+","+page);
			int count =0;
			while(rs.next()){	
				count++;
				Article article = articleService.mapResultSetToArticle(rs);
//				if(article.getArticleId()< begin)
//					continue;
				List<Author> authors = authorService.getAuthorListByArticleId(article.getArticleId());
				//HERE CHANGE AUTHOR TO CITATION AUTHOR OBJECT TO FIT IN THE QUERY CONSTRUCTION METHOD
				List<articlesdata.citation.Author> authorsC = new ArrayList<articlesdata.citation.Author>(authors.size());
				for(Author author: authors){
					articlesdata.citation.Author ac = new articlesdata.citation.Author();
					ac.setSurname(author.getLastName());
					ac.setGivenNameString(author.getFirstName());
					authorsC.add(ac);
				}
				
				List<PMArticle> pmArticles = searcher.getPMArticlesByQuery(article.getTitle(), authorsC);
				for(PMArticle pma: pmArticles){
					MappingStatus status = new MappingStatus(pma.getPmid(), article.getArticleId());
					ElsevierPMComparison.isElsevierAndPMArticleEqual(article, authors, pma, status);
					boolean matched = ElsevierPMComparison.isEqual(status);
					if(matched){
						insertMapping(status, matched);
						break;
					}
				}
			}
			rs.close();
			hasMore = (count==page);
			start += page;
		}
		LOGGER.info("===Task Finished.===");
	}
	
	public void insertMapping(MappingStatus status, boolean matched) throws SQLException{
		
		if(matched)
			LOGGER.info(status.getArticleId()+"--> "+status.getCitationId());
		prepInsertMapping.setInt(1, status.getArticleId());
		prepInsertMapping.setLong(2, status.getCitationId());
		prepInsertMapping.setInt(3, status.getTitleComp());
		prepInsertMapping.setInt(4, status.getAuthorsComp());
		prepInsertMapping.setInt(5, status.getJournalComp());
		prepInsertMapping.setInt(6, matched?1:0);
		prepInsertMapping.setDate(7, new java.sql.Date(Calendar.getInstance().getTimeInMillis()));
		prepInsertMapping.executeUpdate();
	}
	
	public boolean hasChecked(int articleId) throws SQLException{
		Statement stmt = conn.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(SELECT_MAPPING+articleId);
		boolean checked = rs.next();
		rs.close();
		return checked;
	}
	
	public void closeAllStuff() throws SQLException, IOException{
		if(conn != null)
			conn.close();
		if(searcher !=null)
			searcher.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			ElsevierMedlineMapper mapper = new ElsevierMedlineMapper();
			mapper.mapElsevierToMedline(Integer.parseInt(args[0]));
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
