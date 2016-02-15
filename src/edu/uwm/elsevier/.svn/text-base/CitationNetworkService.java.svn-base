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
public class CitationNetworkService {
	
	private CitationNetworkDAO citationNetworkDAO;
	
	public CitationNetworkService() throws ClassNotFoundException, SQLException{
		citationNetworkDAO = new CitationNetworkDaoJdbcImpl();
	}
	
	public List<Long> getTargetCitationIdByCitationId(long citationId)throws SQLException{
		return citationNetworkDAO.getTargetCitationIdByCitationId(citationId);
	}
	
	public List<Long> getTargetCitationIdByCitationIdList(List<Long> citationIds) throws SQLException{
		return citationNetworkDAO.getTargetCitationIdByCitationIdList(citationIds);
	}
	
	public List<Integer> getInciteArticleIdsForArticleId(int articleId) throws SQLException{
		return citationNetworkDAO.getInciteArticleIdsForArticleId(articleId);
	}
	
	public List<Long> getInciteCitationIdsByArticleId(int articleId) throws SQLException{
		return citationNetworkDAO.getInciteCitationIdsByArticleId(articleId);
	}
	
	public void insertMergedCitation(long root, List<Long> equavilentCitationIds) throws SQLException{
		citationNetworkDAO.insertMergedCitation(root, equavilentCitationIds);
	}
	
	public ArticlesDataDBConnection getCurrentConnection(){
		return citationNetworkDAO.getCurrentConnection();
	}
	public long getMaxCitationId() throws SQLException{
		return citationNetworkDAO.getMaxCitationId();
	}
	
	public int getMappingTableMatchedCount()throws SQLException{
		return citationNetworkDAO.getMappingTableMatchedCount();
	}
	
	public int getMergingTableMatchedCount()throws SQLException{
		return citationNetworkDAO.getMergingTableMatchedCount();
	}
	
	public int getElsMLTableMatchedCount() throws SQLException{
		return citationNetworkDAO.getElsMLTableMatchedCount();
	}
	
	public int getMappingTableCount()throws SQLException{
		return citationNetworkDAO.getMappingTableCount();
	}
	
	public int getMergingTableCount()throws SQLException{
		return citationNetworkDAO.getMergingTableCount();
	}
	
	public int getElsMLTableCount() throws SQLException{
		return citationNetworkDAO.getElsMLTableCount();
	}
	
	public int getCitationCount()throws SQLException{
		return citationNetworkDAO.getCitationCount();
	}
	
	public int getArticleCount() throws SQLException{
		return citationNetworkDAO.getArticleCount();
	}
	
	public List<Long> getUnmertedTargetCitationIds(long root, long citationId) throws SQLException{
		return citationNetworkDAO.getUnmertedTargetCitationIds(	root, citationId);
	}
	
	public long getRootByCitationId(long citationId) throws SQLException{
		return citationNetworkDAO.getRootByCitationId(citationId);
	}
	
	public int getMergingTableCitationCount() throws SQLException{
		return citationNetworkDAO.getMergingTableCitationCount();
	}
	
	public Date getArticleYearByArticleId(int article_id) throws SQLException{
		return citationNetworkDAO.getArticleYearByArticleId(article_id);
	}
	
	public List<Integer> getInciteMedlineArticleIdsForArticleId(int articleId)throws SQLException{
		return citationNetworkDAO.getInciteMedlineArticleIdsForArticleId(articleId);
	}
	
	public long getPMIDByArticleId(int articleId)throws SQLException{
		return citationNetworkDAO.getPMIDByArticleId(articleId);
	}
	
	public List<Integer> getOutciteMedlineArticleIdsByArticleId(int articleId) throws SQLException{
		return citationNetworkDAO.getOutciteMedlineArticleIdsByArticleId(articleId);
	}
	
	public int getArticleIdByInciteCitationId(long citationId)throws SQLException{
		return citationNetworkDAO.getArticleIdByInciteCitationId(citationId);
	}
	
	public Set<Long> getAllEquavilentCitationsForCitation(long citationId) throws SQLException{
		return citationNetworkDAO.getAllEquavilentCitationsForCitation(citationId);
	}
	
	public MedlineAritcleNodeStatistiscs getMedlineAritcleNodeStatistiscsByArticleId(int articleId) throws SQLException{
		return citationNetworkDAO.getMedlineAritcleNodeStatistiscsByArticleId(articleId);
	}
	
	public MedlineAritcleNodeStatistiscs getMedlineAritcleNodeStatistiscsByPmid(long pmid) throws SQLException{
		return citationNetworkDAO.getMedlineAritcleNodeStatistiscsByPmid(pmid);
	}
	
	public List<Date> getArticleYearByArticleIds(List<Integer> articleIds)throws SQLException{
		return citationNetworkDAO.getArticleYearByArticleIds(articleIds);
	}
	
	public int getMeshIdByMesh(String mesh)throws SQLException{
		return citationNetworkDAO.getMeshIdByMesh(mesh);
	}
	
	public int getYearByPMID(long pmid)throws SQLException{
		return citationNetworkDAO.getYearByPMID(pmid);
	}
	
	public int getMArticleCountByYear(int year)throws SQLException{
		return citationNetworkDAO.getMArticleCountByYear(year);
	}
	
	public int getMAuthorCountByYear(int year)throws SQLException{
		return citationNetworkDAO.getMAuthorCountByYear(year);
	}
	
	public List<Integer> getArticleIdByPMID(long pmid) throws SQLException{
		return citationNetworkDAO.getArticleIdByPMID(pmid);
	}
	
	public void closeService()throws SQLException{
		citationNetworkDAO.closeService();
	}
	
	public List<Long> getIncitePMIDForArticleIdBetweenYear(int articleId, int start, int end) throws SQLException{
		return citationNetworkDAO.getIncitePMIDForArticleIdBetweenYear(articleId, start, end);
	}
	
	public List<Integer> getInciteArticleIdsForArticleIdBetweenYear(int articleId, int start, int end)throws SQLException{
		return citationNetworkDAO.getInciteArticleIdsForArticleIdBetweenYear(articleId, start, end);
	}
}
