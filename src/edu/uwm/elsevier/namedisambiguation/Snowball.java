/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class Snowball {

	private ArticlesDataDBConnection databaseConnection;
	private CitationNetworkService citationNetworkService;
	private AuthorDSBService authorDSBService;
	private AuthorService authorService;
	private static String INSERT_MAPPING = 
			"insert into authority_map_v2 (authority_author_id, author_id, pmid, is_original) values(?, ?, ?, ?)";
	private PreparedStatement prepInsertMap ;
	private long maxPMID =19602982;
	private Logger logger = Logger.getLogger(Snowball.class);
	public Snowball() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
		citationNetworkService = new CitationNetworkService();
		authorService = new AuthorService();
		prepInsertMap = databaseConnection.getConnection().prepareStatement(INSERT_MAPPING);
		authorDSBService = new AuthorDSBService();
	}
	
	public void dsb(int start) throws SQLException{
		logger.info("Task start. author name dsb by self citation...");
		int lower = start;
		int page = 1000;
		int upper = lower+page;
		
		String inclouse = "pmid not in (select pmid from authority_map_v2";
		String sql = "select pmid, article_id from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = null;
		while(lower < maxPMID){
			logger.info("lower="+lower);
			rs = stmt.executeQuery(sql+" where pmid>"+lower+" and pmid<="+upper+" and "+inclouse+"  where pmid>"+lower+" and pmid<="+upper +")");
			while(rs.next()){
				long pmid = rs.getLong(1);
				int articleId = rs.getInt(1);
				findAuthorityIdBySelfCitation(articleId, pmid);
			}
			prepInsertMap.executeBatch();
			rs.close();
			lower = upper;
			upper += page;
		}
		logger.info("Task done.");
	}
	
	public void findAuthorityIdBySelfCitation(int articleId, long pmid) throws SQLException{
		List<Integer> incites = citationNetworkService.getInciteMedlineArticleIdsForArticleId(articleId);
		List<Author> authors = authorService.getAuthorListByArticleIdList(incites);
		
		List<Integer> outcites = citationNetworkService.getOutciteMedlineArticleIdsByArticleId(articleId);
		List<Author> outAuthors = authorService.getAuthorListByArticleIdList(outcites);
		if(!authors.isEmpty()){
			authors.addAll(outAuthors);
		}else
			authors = outAuthors;
		List<Author> authorsToDecide = authorService.getAuthorListByArticleId(articleId);
		for(Author author: authorsToDecide){
			for(Author dest: authors){
				if(AuthorityTool.isNamesEqual(author, dest)){
					// found a self-citation. map the author-to-decide to dest author authority_id
					String authorityId = authorDSBService.getAuthorityIdByAuthorId(dest.getAuthorId());
					if(authorityId !=null){
						AuthorityEntity entity = new AuthorityEntity();
						entity.setAuthorId(author.getAuthorId());
						entity.setAuthorityId(authorityId);
						entity.setPmid(pmid);
						entity.setIsOriginal(0);
						insertMap(entity);
						break;
					}
				}
			}
		}
	}
	
	
	public void insertMap(AuthorityEntity entity) throws SQLException{
		prepInsertMap.setString(1, entity.getAuthorityId());
		prepInsertMap.setLong(2, entity.getAuthorId());
		prepInsertMap.setLong(3, entity.getPmid());
		prepInsertMap.setInt(4, entity.getIsOriginal());
		prepInsertMap.addBatch();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Snowball snowball = new Snowball();
			snowball.dsb(Integer.parseInt(args[0]));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
