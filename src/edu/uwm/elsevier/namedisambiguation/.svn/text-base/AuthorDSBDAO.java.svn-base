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
public interface AuthorDSBDAO {
	public List<Integer> getAuthorIdsByAuthorityID(String authorityId) throws SQLException;
	
	public String getAuthorityIdByAuthorId(long authorId) throws SQLException;
	
	public List<String> getCoAuthorsByAuthorId(long authorId)throws SQLException;
	
	
	public List<String> getCoAuthorsByAuthorityId(String authorityId) throws SQLException;
	
	public List<String> getCoAuthorsByAuthorityIdBetweenYears(String authorityId, int start, int end) throws SQLException;
	
	public List<String> getAuthorsByArticleId(int articleId) throws SQLException;
	
	public List<String> getAuthorsByPmid(long pmid) throws SQLException;
	
	public boolean isCoAuthor(String src, String dest)throws SQLException;
	/**
	 * AuthorityId -> authorId -> articleId
	 * @param authorityId
	 * @return
	 * @throws SQLException
	 */
	public List<Integer> getArticleIdsByAuthorityId(String authorityId)throws SQLException;
	/**
	 * AuthorityId -> PMID (-> articleId)
	 * @param authorityId
	 * @return
	 * @throws SQLException
	 */
	public List<Long> getPmidsByAuthorityId(String authorityId)throws SQLException;
	
	public List<Long> getPmidsBetweenYearsByAuthorityId(String authorityId, int start, int end) throws SQLException;
	
	public String getYearStringByAuthorityId(String authorityId)throws SQLException;
	
	public int getYearSpanByAuthorityId(String authorityId)throws SQLException;
	
	public void closeService()throws SQLException;
	
	public String getAuthorNameByAuthorityId(String authorityId)throws SQLException;
	
	public List<String> getAuthorLastNameByAuthorityId(String authorityId)throws SQLException;
	
	public List<String> getAuthorFirstNameByAuthorityId(String authorityId)throws SQLException;
	
	public List<String> getAuthorMiddleInitialByAuthorityId(String authorityId)throws SQLException;
	
	public float getClusteringCoefByAuthorityId(String authorityId)throws SQLException;
	
	public float getClusteringCoefByAuthorityIdBetweenYears(String authorityId, int start, int end) throws SQLException;
	
	
}
