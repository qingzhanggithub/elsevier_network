/**
 * 
 */
package edu.uwm.elsevier;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public interface CitationNetworkDAO {
	/**
	 * For table citation_merger
	 * @param citationId
	 * @return
	 */
	public List<Long> getTargetCitationIdByCitationId(long citationId)throws SQLException;
	
	public List<Long> getTargetCitationIdByCitationIdList(List<Long> citationIds) throws SQLException;
	
	/**
	 * For table citation_merger
	 * @param citationId
	 * @return
	 */
	public List<Long> getCitationIdsByTergetCitationId(long citationId)throws SQLException;
	
	/**
	 * For table cnetworkv4_enhanced
	 * @param citationId
	 * @return
	 */
	public int getArticleIdByInciteCitationId(long citationId)throws SQLException;
	/**
	 * For table cnetworkv4_enhanced
	 * @param articleId
	 * @return
	 */
	public List<Long> getInciteCitationIdsByArticleId(int articleId)throws SQLException;
	
	public List<Integer> getInciteArticleIdsForArticleId(int articleId)throws SQLException;
	
	public List<Integer> getInciteMedlineArticleIdsForArticleId(int articleId)throws SQLException;
	
	/**
	 * For table citation_merged
	 * @param articleId
	 * @return
	 */
	public List<Long> getOutciteCitationIdByArticleId(int articleId)throws SQLException;
	
	public void insertMergedCitation(long root, List<Long> equavilentCitationIds) throws SQLException;
	
	public ArticlesDataDBConnection getCurrentConnection();
	
	public long getMaxCitationId() throws SQLException;
	
	public int getMappingTableMatchedCount()throws SQLException;
	
	public int getMergingTableMatchedCount()throws SQLException;
	
	public int getElsMLTableMatchedCount() throws SQLException;
	
	public int getMappingTableCount()throws SQLException;
	
	public int getMergingTableCount()throws SQLException;
	
	public int getElsMLTableCount() throws SQLException;
	
	public int getCitationCount()throws SQLException;
	
	public int getArticleCount() throws SQLException;
	
	public List<Long> getUnmertedTargetCitationIds(long root, long citationId) throws SQLException;
	
	public long getRootByCitationId(long citationId) throws SQLException;
	
	public int getMergingTableCitationCount() throws SQLException;
	
	public Date getArticleYearByArticleId(int article_id) throws SQLException;
	public List<Date> getArticleYearByArticleIds(List<Integer> articleIds)throws SQLException;
	
	public long getPMIDByArticleId(int articleId)throws SQLException;
	
	public List<Integer> getOutciteMedlineArticleIdsByArticleId(int articleId) throws SQLException;
	
	public Set<Long> getAllEquavilentCitationsForCitation(long citationId) throws SQLException;
	
	public Set<Long> getAllEquavilentCitationsFromMergedResult(long citationid) throws SQLException;
	
	public MedlineAritcleNodeStatistiscs getMedlineAritcleNodeStatistiscsByArticleId(int articleId) throws SQLException;
	
	public MedlineAritcleNodeStatistiscs getMedlineAritcleNodeStatistiscsByPmid(long pmid) throws SQLException;
	
	public int getMeshIdByMesh(String mesh)throws SQLException;
	
	public int getYearByPMID(long pmid)throws SQLException;
	
	public int getMArticleCountByYear(int year)throws SQLException;
	
	public int getMAuthorCountByYear(int year)throws SQLException;
	
	public List<Integer> getArticleIdByPMID(long pmid)throws SQLException;
	
	public List<Long> getIncitePMIDForArticleIdBetweenYear(int articleId, int start, int end)throws SQLException;
	
	public List<Integer> getInciteArticleIdsForArticleIdBetweenYear(int articleId, int start, int end)throws SQLException;
	
	public void closeService()throws SQLException;

}
