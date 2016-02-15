/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.namedisambiguation.AuthorityTool;

import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class CoauthorJDBC {
	
	private ArticlesDataDBConnection databaseConnection ;
	private AuthorDSBService authorDSBService;
	private CitationNetworkService citationNetworkService;
	private AuthorService authorService;
	private int yearCutoff;
	
	public CoauthorJDBC(int yearCutoff) throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
		citationNetworkService = new CitationNetworkService();
		authorService = new AuthorService();
		authorDSBService = new AuthorDSBService();
		this.yearCutoff = yearCutoff;
	}
	
	public List<PublicationAttribute> getPublicationForAuthorPair(AuthorshipEdge edge, int start, int end) throws SQLException{
		String sql = "select pmid, year from "+ITableNames.CO_AUTHOR+
				" where authority_author_id =\'"+AuthorityTool.escape(edge.src)+"\'" +
						" and co_author_authority_author_id = \'"+AuthorityTool.escape(edge.dest)+"\'";
		if(start != -1)
			sql += " and yaer >="+start;
		if(end != -1)
			sql += " and year <" +end;
		
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<PublicationAttribute> pubList = new ArrayList<PublicationAttribute>();
		while(rs.next()){
			PublicationAttribute pub = new PublicationAttribute(rs.getLong(1), rs.getInt(2));
			pubList.add(pub);
		}
		rs.close();
		stmt.close();
		return pubList;
	}
	
	
	public Author getCorrespondingAuthorForPMID(long pmid) throws SQLException{
		List<Integer> articleIds = citationNetworkService.getArticleIdByPMID(pmid);
		if(articleIds.size() ==1){
			List<Author> authors = authorService.getAuthorListByArticleId(articleIds.get(0));
			for(Author author: authors){
				if(author.isCorrespondingAuthor())
					return author;
			}
		}
		return null;
	}
	
	public Author getAuthorByAuthorityIdAndPMID(String authorityId, long pmid) throws SQLException{
		List<Integer> articleIds = citationNetworkService.getArticleIdByPMID(pmid);
		if(articleIds.size() ==1){
			List<Integer> authorIds = authorDSBService.getAuthorIdsByAuthorityID(authorityId);
			List<Author> authors = authorService.getAuthorListByArticleId(articleIds.get(0));
			for(Author author: authors){
				for(int authorId: authorIds){
					if(author.getAuthorId() == authorId)
						return author;
				}
			}
		}
		return null;
	}
	
	public void closeDBConnection() throws SQLException{
		if(databaseConnection != null)
			databaseConnection.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
