/**
 * 
 */
package edu.uwm.elsevier;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class CitationNetworkDaoJdbcImpl implements CitationNetworkDAO {

	private static String SELECT_TARGET_CITATION_ID = "select target_citation_id from "+ITableNames.CITATION_MERGING_TABLE;
	private static String SEELCT_ARTICLE_ID = "select article_id from "+ITableNames.CITATION_MAPPING_TABLE;
	private ArticlesDataDBConnection databaseConnection ;
	private Logger LOGGER = NetworkBuilderLogger.getLogger("CitationNetworkDaoJdbcImpl");
	
	public CitationNetworkDaoJdbcImpl() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	@Override
	public List<Long> getTargetCitationIdByCitationId(long citationId) throws SQLException {
		LOGGER.info("Getting target citation id for citation_id:"+citationId);
		String sql = SELECT_TARGET_CITATION_ID +" where citation_id = "+citationId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> targetCitationIds = new ArrayList<Long>();
		while(rs.next()){
			if(rs.getLong(1) > citationId)
				targetCitationIds.add(rs.getLong(1));
		}
		rs.close();
		stmt.close();
		return targetCitationIds;
	}
	
	@Override
	public List<Long> getUnmertedTargetCitationIds(long root, long citationId)
			throws SQLException {
		String sql = SELECT_TARGET_CITATION_ID +" where citation_id = "+citationId+" and target_citation_id not in (select citation_id from "+ ITableNames.MERGED_CITATION_TABLE+" where root_citation_id="+root+")";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> targetCitationIds = new ArrayList<Long>();
		while(rs.next()){
			if(rs.getLong(1) > citationId)
				targetCitationIds.add(rs.getLong(1));
		}
		rs.close();
		stmt.close();
		return targetCitationIds;
	}

	@Override
	public List<Long> getCitationIdsByTergetCitationId(long citationId) throws SQLException {
		LOGGER.info("Getting citation_id by target_citation_id:"+citationId);
		String sql = "select citation_id from "+ITableNames.CITATION_MERGING_TABLE+" where target_citation_id="+citationId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> citationIds = new ArrayList<Long>();
		while(rs.next()){
				citationIds.add(rs.getLong(1));
		}
		rs.close();
		stmt.close();
		return citationIds;
	}

	@Override
	public int getArticleIdByInciteCitationId(long citationId) throws SQLException {
		String sql = "select article_id from "+
					ITableNames.CITATION_MAPPING_TABLE +" where citation_id ="+citationId+" and is_matched=1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int articleId = -1;
		if(rs.next()){
		articleId = rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return articleId;
	}

	@Override
	public List<Long> getInciteCitationIdsByArticleId(int articleId) throws SQLException {
		String sql = "select citation_id from "+ITableNames.CITATION_MAPPING_TABLE+" where article_id ="+articleId+" and is_matched=1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> inciteCitationIds = new ArrayList<Long>();
		while(rs.next()){
			inciteCitationIds.add(rs.getLong(1));
		}
		rs.close();
		stmt.close();
		return inciteCitationIds;
	}

	@Override
	public List<Integer> getInciteArticleIdsForArticleId(int articleId) throws SQLException {
		String sql = "select distinct c.article_id from " +
				ITableNames.CITATION_TABLE +" as c, " +ITableNames.CITATION_MAPPING_TABLE+" as m "+
				" where m.citation_id = c.citation_id and m.article_id ="+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Integer> inciteArticleIds = new ArrayList<Integer>();
		while(rs.next()){
			inciteArticleIds.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return inciteArticleIds;
	}

	@Override
	public List<Long> getOutciteCitationIdByArticleId(int articleId) {
		return null;
	}


	@Override
	public List<Long> getTargetCitationIdByCitationIdList(List<Long> citationIds)
			throws SQLException {
		StringBuffer in = new StringBuffer();
		for(long citationId: citationIds){
			in.append(',').append(citationId);
		}
		in.append(" )");
		String inStr = in.toString();
		String sql = SELECT_TARGET_CITATION_ID + "where citation_id in "+inStr.replaceFirst(",", "(");
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> targetCitationIds = new ArrayList<Long>();
		while(rs.next()){
			targetCitationIds.add(rs.getLong(1));
		}
		rs.close();
		stmt.close();
		return targetCitationIds;
	}

	@Override
	public void insertMergedCitation(long root,
			List<Long> equavilentCitationIds) throws SQLException {
		String sql = "insert into merged_citation (citation_id, root_citation_id) values (?, ?)";
		PreparedStatement prep = databaseConnection.getConnection().prepareStatement(sql);
		for(long eqCitationId: equavilentCitationIds){
			prep.setLong(1, eqCitationId);
			prep.setLong(2, root);
			prep.addBatch();
		}
		prep.executeBatch();
		prep.close();
	}

	@Override
	public ArticlesDataDBConnection getCurrentConnection() {
		return databaseConnection;
	}

	@Override
	public long getMaxCitationId() throws SQLException {
		String sql = "select max(citation_id) from "+ ITableNames.CITATION_TABLE;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public int getMappingTableMatchedCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.CITATION_MAPPING_TABLE+" where is_matched=1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public int getMergingTableMatchedCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.CITATION_MERGING_TABLE+" where is_matched=1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public int getElsMLTableMatchedCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE+" where is_matched =1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public int getMappingTableCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.CITATION_MAPPING_TABLE +" where is_matched =1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public int getMergingTableCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.CITATION_MERGING_TABLE +" where is_matched=1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public int getElsMLTableCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE +" where is_matched =1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public int getCitationCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.CITATION_DETAIL_TABLE +" where is_originally_parsed =1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
		
	}

	@Override
	public int getArticleCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE+" where is_matched =1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public long getRootByCitationId(long citationId) throws SQLException {
		String sql = "select root_citation_id from "+ITableNames.MERGED_CITATION_TABLE+" where citation_id="+citationId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		long root = -1;
		if(rs.next()){
			root = rs.getLong(1);
		}else{
			sql = "select root_citation_id from "+ITableNames.MERGED_CITATION_TABLE+" where root_citation_id="+citationId;
			rs = stmt.executeQuery(sql);
			if(rs.next())
				root = citationId;
		}
		rs.close();
		stmt.close();
		
		return root;
	}

	@Override
	public int getMergingTableCitationCount() throws SQLException {
		String sql = "select count(*) from "+ITableNames.MERGED_CITATION_TABLE;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		int result = rs.getInt(1);
		rs.close();
		stmt.close();
		return result;
	}

	@Override
	public Date getArticleYearByArticleId(int article_id) throws SQLException {
		LOGGER.info("Getting year for articleId:"+article_id);
		String sql = "select date_published from "+ITableNames.ARTICLE_TABLE+" where article_id="+article_id;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		Date d = null;
		if(rs.next()){
			d = rs.getDate(1);
		}
		rs.close();
		stmt.close();
		return d;
	}

	@Override
	public List<Integer> getInciteMedlineArticleIdsForArticleId(int articleId)
			throws SQLException {
		String sql = "select distinct c.article_id from " +
				ITableNames.CITATION_TABLE +" as c, " +
				ITableNames.CITATION_MAPPING_TABLE+" as m, "+
				ITableNames.ELSEVIER_PMID_MAPPING_TABLE +" as epm "+
				" where m.citation_id = c.citation_id and m.article_id ="+articleId+
				" and c.article_id = epm.article_id and m.is_matched=1 and epm.is_matched =1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Integer> inciteArticleIds = new ArrayList<Integer>();
		while(rs.next()){
			inciteArticleIds.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return inciteArticleIds;
	}

	@Override
	public long getPMIDByArticleId(int articleId) throws SQLException {
		String sql = "select pmid from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE+
				" where article_id="+articleId+" and is_matched=1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		long pmid = -1;
		if(rs.next()){
			pmid = rs.getLong(1);
		}
		rs.close();
		stmt.close();
		return pmid;
	}

	@Override
	public List<Integer> getOutciteMedlineArticleIdsByArticleId(int articleId)
			throws SQLException {
		String sql = "select distinct dest_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+" where src_article_id= "+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Integer> outciteList = new ArrayList<Integer>();
		while(rs.next()){
			outciteList.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return outciteList;
	}
	/**
	 * For the purpose that merging table and merged table are inconsistant. It is the super set of all equavilent citations.
	 */
	@Override
	public Set<Long> getAllEquavilentCitationsForCitation(long citationId)
			throws SQLException {
		
		Set<Long> equ = getAllEquavilentCitationsFromMergedResult(citationId);
		List<Long> targets = getTargetCitationIdByCitationId(citationId);
		List<Long> srcs = getCitationIdsByTergetCitationId(citationId);
		equ.addAll(targets);
		equ.addAll(srcs);
		return equ;
	}

	@Override
	public Set<Long> getAllEquavilentCitationsFromMergedResult(
			long citationid) throws SQLException {
		String rootSql =" select root_citation_id from "+ITableNames.MERGED_CITATION_TABLE+" where citation_id="+citationid;
		String sql = "select citation_id from "+ITableNames.MERGED_CITATION_TABLE+" where root_citation_id =";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(rootSql);
		ResultSet subRs;
		Set<Long> ids = new HashSet<Long>();
		Statement subStmt = databaseConnection.getConnection().createStatement();
		while(rs.next()){
//			LOGGER.info("for root_citation_id:"+rs.getLong(1));
			subRs = subStmt.executeQuery(sql+rs.getLong(1));
			while(subRs.next()){
//				LOGGER.info("for citation_id:"+subRs.getLong(1));
				ids.add(subRs.getLong(1));
			}
			subRs.close();
		}
		subStmt.close();
		rs.close();
		stmt.close();
		return ids;
	}

	@Override
	public MedlineAritcleNodeStatistiscs getMedlineAritcleNodeStatistiscsByArticleId(
			int articleId) throws SQLException {
//		LOGGER.info("Getting medline article node stats by articleId:"+articleId);
		String sql = "select * from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" where article_id ="+articleId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		MedlineAritcleNodeStatistiscs stats = null;
		if(rs.next()){
			stats = mapRsToMedlineAritcleNodeStatistiscs(rs);
			
		}
		rs.close();
		stmt.close();
		return stats;
	}

	private MedlineAritcleNodeStatistiscs mapRsToMedlineAritcleNodeStatistiscs(ResultSet rs) throws SQLException{
		MedlineAritcleNodeStatistiscs stats = new MedlineAritcleNodeStatistiscs();
		stats.setArticleId(rs.getInt("article_id"));
		stats.setPmid(rs.getLong("pmid"));
		stats.setIncites(rs.getInt("incite"));
		stats.setIncitesFromMedline(rs.getInt("incite_from_medline"));
		stats.setOutcites(rs.getInt("outcite"));
		stats.setOutcitesToMedline(rs.getInt("outcite_to_medline"));
		stats.setNumOfAuthors(rs.getInt("num_of_authors"));
		stats.setYear(rs.getInt("year"));
		stats.setMeshs(rs.getString("meshs"));
		return stats;
	}

	@Override
	public MedlineAritcleNodeStatistiscs getMedlineAritcleNodeStatistiscsByPmid(
			long pmid) throws SQLException {
		String sql = "select * from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" where pmid ="+pmid;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		MedlineAritcleNodeStatistiscs stats = null;
		if(rs.next()){
			stats = mapRsToMedlineAritcleNodeStatistiscs(rs);
		}
		rs.close();
		stmt.clearBatch();
		return stats;
	}

	@Override
	public List<Date> getArticleYearByArticleIds(List<Integer> articleIds)
			throws SQLException {
		
//		LOGGER.info("Getting year for articleIds:"+articleIds.toString());
		List<Date> dateList =new ArrayList<Date>();
		String sql = "select date_published from "+ITableNames.ARTICLE_TABLE+" where article_id in ";
		Statement stmt = databaseConnection.getConnection().createStatement();
		StringBuffer sb = new StringBuffer();
		boolean isfirst = true;
		for(int articleId : articleIds){
			if(isfirst){
				sb.append(articleId);
				isfirst=false;
			}else
				sb.append(",").append(articleId);
		}
		String years = sb.toString();
		if(years.length()>0){
			ResultSet rs = stmt.executeQuery(sql+"("+sb.toString()+")");
			while(rs.next()){
				Date d = rs.getDate(1);
				dateList.add(d);
			}
			rs.close();
		}
		stmt.close();
		return dateList;
	}

	@Override
	public int getMeshIdByMesh(String mesH) throws SQLException {
		String sql = "select mesh_id from "+ITableNames.MESH_STATS+" where mesh = \'"+addEscapeToSql(mesH)+"\'";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int meshId = -1;
		if(rs.next()){
			meshId = rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return meshId;
	}
	
	private String addEscapeToSql(String org){
		char[] chars = org.toCharArray();
		StringBuffer sb = new StringBuffer();
		for(char c: chars){
			if(c == '\'')
				sb.append('\\').append('\'');
			else
				sb.append(c);
		}
		return sb.toString();
	}
	
	public static void main(String[] args){
		try {
			CitationNetworkDaoJdbcImpl networkjdbc = new CitationNetworkDaoJdbcImpl();
			int meshId = networkjdbc.getMeshIdByMesh(args[0]);
			System.out.println(args[0]+"\tmesh_id="+meshId);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public int getYearByPMID(long pmid) throws SQLException {
		String sql = "select year from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" where pmid="+pmid;
		int year = -1;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		if(rs.next()){
			year = rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return year;
	}

	@Override
	public int getMArticleCountByYear(int year) throws SQLException {
		String sql = "select count(distinct pmid) from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" where year="+year;
		int count =0;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		if(rs.next()){
			count = rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return count;
	}

	@Override
	public int getMAuthorCountByYear(int year) throws SQLException {
		String sql =" select count(distinct authority_author_id) from "+ITableNames.AUTHOR_YEARS+" where years like \'%"+year+"%\'";
		int count=0;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		if(rs.next()){
			count = rs.getInt(1);
		}
		rs.close();
		stmt.close();
		return count;
	}

	@Override
	public List<Integer> getArticleIdByPMID(long pmid) throws SQLException {
		String sql = "select distinct article_id from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE +" where pmid="+pmid;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Integer> articleIds = new ArrayList<Integer>();
		while(rs.next()){
			articleIds.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return articleIds;
	}
	
	public void closeService()throws SQLException{
		databaseConnection.close();
	}

	@Override
	public List<Long> getIncitePMIDForArticleIdBetweenYear(int articleId, int start, int end) throws SQLException {
		String sql = "select distinct pmid from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" stat, "+ITableNames.MEDLINE_NETWORK_TABLE+
				" where dest_article_id = "+articleId+" and src_article_id = stat.article_id ";
		if(start != -1)
			sql += " and year >= "+start;
		if(end != -1)
			sql += " and year < "+end;
		
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> pmids = new ArrayList<Long>();
		while(rs.next()){
			pmids.add(rs.getLong(1));
		}
		rs.close();
		stmt.close();
		return pmids;
	}

	@Override
	public List<Integer> getInciteArticleIdsForArticleIdBetweenYear( int articleId, int start, int end) throws SQLException {
		String sql = "select distinct stat.article_id from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" stat, "+ITableNames.MEDLINE_NETWORK_TABLE+
				" where dest_article_id = "+articleId+" and src_article_id = stat.article_id ";
		if(start != -1)
			sql += " and year >= "+start;
		if(end != -1)
			sql += " and year < "+end;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Integer> articleIds = new ArrayList<Integer>();
		while(rs.next()){
			articleIds.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return articleIds;
	}

}
