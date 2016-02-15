/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.sql.SQLException;
import java.util.List;

/**
 * @author qing
 *
 */
public class AuthorDSBService {
	
	private AuthorDSBDAO authorDSBDao;
	
	public AuthorDSBService() throws ClassNotFoundException, SQLException{
		authorDSBDao = new AuthorDSBDAOJdbcImpl();
	}
	
	public List<Integer> getAuthorIdsByAuthorityID(String authorityId)
			throws SQLException{
		return authorDSBDao.getAuthorIdsByAuthorityID(authorityId);
	}
	
	public String getAuthorityIdByAuthorId(long authorId) throws SQLException{
		return authorDSBDao.getAuthorityIdByAuthorId(authorId);
	}
	
	public List<String> getCoAuthorsByAuthorId(long authorId)throws SQLException{
		return authorDSBDao.getCoAuthorsByAuthorId(authorId);
	}
	
	public List<String> getCoAuthorsByAuthorityId(String authorityId) throws SQLException{
		return authorDSBDao.getCoAuthorsByAuthorityId(authorityId);
	}
	
	public List<String> getAuthorsByArticleId(int articleId) throws SQLException{
		return authorDSBDao.getAuthorsByArticleId(articleId);
	}
	
	public List<String> getAuthorsByPmid(long pmid) throws SQLException{
		return authorDSBDao.getAuthorsByPmid(pmid);
	}
	
	public boolean isCoAuthor(String src, String dest) throws SQLException{
		return authorDSBDao.isCoAuthor(src, dest);
	}
	
	public List<Integer> getArticleIdsByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getArticleIdsByAuthorityId(authorityId);
	}
	
	public List<Long> getPmidsByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getPmidsByAuthorityId(authorityId);
	}
	
	public String getYearStringByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getYearStringByAuthorityId(authorityId);
	}
	
	public int getYearSpanByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getYearSpanByAuthorityId(authorityId);
	}
	public void closeService()throws SQLException{
		authorDSBDao.closeService();
	}
	
	public String getAuthorNameByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getAuthorNameByAuthorityId(authorityId);
	}
	
	public List<String> getAuthorLastNameByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getAuthorLastNameByAuthorityId(authorityId);
	}
	
	public List<String> getAuthorFirstNameByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getAuthorFirstNameByAuthorityId(authorityId);
	}
	
	public List<String> getAuthorMiddleInitialByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getAuthorMiddleInitialByAuthorityId(authorityId);
	}
	
	public List<Long> getPmidsBetweenYearsByAuthorityId(String authorityId, int start, int end) throws SQLException{
		return authorDSBDao.getPmidsBetweenYearsByAuthorityId(authorityId, start, end);
	}
	
	public float getClusteringCoefByAuthorityId(String authorityId)throws SQLException{
		return authorDSBDao.getClusteringCoefByAuthorityId(authorityId);
	}
	
	public List<String> getCoAuthorsByAuthorityIdBetweenYears(String authorityId, int start, int end) throws SQLException{
		return authorDSBDao.getCoAuthorsByAuthorityIdBetweenYears(authorityId, start, end);
	}
	
	public float getClusteringCoefByAuthorityIdBetweenYears(String authorityId, int start, int end) throws SQLException{
		return authorDSBDao.getClusteringCoefByAuthorityIdBetweenYears(authorityId, start, end);
	}

}
