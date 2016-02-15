/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.authoranalysis.MedlineAuthorAnalysis;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class AuthorDSBDAOJdbcImpl implements AuthorDSBDAO {

	private ArticlesDataDBConnection articleDBConnection;
	private AuthorityDatabaseConnection authorityDBConnection;
	
	public AuthorDSBDAOJdbcImpl() throws ClassNotFoundException, SQLException{
		articleDBConnection = ArticlesDataDBConnection.getInstance();
		authorityDBConnection = AuthorityDatabaseConnection.getInstance();
	}
	
	@Override
	public List<Integer> getAuthorIdsByAuthorityID(String authorityId)
			throws SQLException {
		
		String sql = "select distinct author_id from "+ITableNames.AUTHORITY_MAP+" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Integer> authorIds = new ArrayList<Integer>();
		while(rs.next()){
			authorIds.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return authorIds;
	}
	
	@Override
	public String getAuthorityIdByAuthorId(long authorId) throws SQLException {
		String sql = "select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP+" where author_id="+authorId;
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		String authorityId = null;
		if(rs.next()){
			authorityId = rs.getString(1);
		}
		rs.close();
		stmt.close();
		return authorityId;
	}

	@Override
	public List<String> getCoAuthorsByAuthorId(long authorId) throws SQLException {
		String sql = "select distinct co_author_authority_author_id from "+ITableNames.CO_AUTHOR+" co, "+ITableNames.AUTHORITY_MAP+"am where am.author_id="+authorId+" and co.authority_author_id = am.authority_author_id";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> coAuthors = new ArrayList<String>();
		while(rs.next()){
			coAuthors.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return coAuthors;
	}

	@Override
	public List<String> getCoAuthorsByAuthorityId(String authorityId) throws SQLException {
		
		String sql = "select distinct co_author_authority_author_id from "+ITableNames.CO_AUTHOR+" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> coAuthors = new ArrayList<String>();
		while(rs.next()){
			coAuthors.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return coAuthors;
	}
	
	@Override
	public List<String> getCoAuthorsByAuthorityIdBetweenYears(String authorityId, int start, int end)throws SQLException {
		String sql = "select distinct co_author_authority_author_id from "+ITableNames.CO_AUTHOR+" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\'";
		if(start !=-1){
			sql += " and year >="+start;
		}
		if(end !=-1)
			sql += " and year <"+end;
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> coAuthors = new ArrayList<String>();	// already distinct, don't need Set.
		while(rs.next()){
			coAuthors.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return coAuthors;
	}

	@Override
	public List<String> getAuthorsByArticleId(int articleId) throws SQLException {
		return null;
	}

	@Override
	public List<String> getAuthorsByPmid(long pmid) throws SQLException{
		String sql = "select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP+" am ,"+ITableNames.AUTHOR_PMID+
				" ap where am.pmid="+pmid+" and ap.PMID=\'"+pmid+"\' and authority_author_id=authorID";// to make sure it is a right mapping by checking with original authority database.
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> authors = new ArrayList<String>();
		while(rs.next()){
			authors.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return authors;
	}

	@Override
	public boolean isCoAuthor(String src, String dest) throws SQLException {
		String sql = "select * from "+ITableNames.CO_AUTHOR+" where authority_author_id=\'"+AuthorityTool.escape(src)+"\' and co_author_authority_author_id=\'"+AuthorityTool.escape(dest)+"\'";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		boolean isCoauthor = false;
		if(rs.next())
			isCoauthor = true;
		rs.close();
		stmt.close();
		return isCoauthor;
	}

	@Override
	public List<Integer> getArticleIdsByAuthorityId(String authorityId)throws SQLException {
		String sql = "select distinct article_id from "+ITableNames.AUTHOR_TABLE+", "+ITableNames.AUTHORITY_MAP+" authority where authority.authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\' and authority.author_id = author.author_id";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Integer> articleIds = new ArrayList<Integer>();
		while(rs.next()){
			articleIds.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
		return articleIds;
	}

	@Override
	public List<Long> getPmidsByAuthorityId(String authorityId)throws SQLException {
		String sql = "select distinct pmid from "+ITableNames.AUTHORITY_MAP+" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> pmids = new ArrayList<Long>();
		while(rs.next()){
			pmids.add(Long.parseLong(rs.getString(1)));
		}
		rs.close();
		stmt.close();
		return pmids;
	}
	
	@Override
	public List<Long> getPmidsBetweenYearsByAuthorityId(String authorityId, int start, int end) throws SQLException {
		String sql ="select distinct am.pmid from "+ITableNames.AUTHORITY_MAP+" am, "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" med  " +
				" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\' and am.pmid = med.pmid "; 
		if(start != -1 && end != -1)
			sql += "and med.year >="+start+" and med.year<"+end;
		else if(start != -1)
			sql += " and med.year>="+start;
		else
			sql += " and med.year <"+end;
		
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<Long> pmids = new ArrayList<Long>();
		while(rs.next()){
			pmids.add(Long.parseLong(rs.getString(1)));
		}
		rs.close();
		stmt.close();
		return pmids;
	}

	@Override
	public String getYearStringByAuthorityId(String authorityId)throws SQLException {
		String sql = "select years from "+ITableNames.AUTHOR_YEARS+" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		String years = null;
		if(rs.next()){
			years = rs.getString(1);
		}
		rs.close();
		stmt.close();
		return years;
	}

	@Override
	public int getYearSpanByAuthorityId(String authorityId) throws SQLException {
		String sql = "select year from "+ITableNames.AUTHORITY_MAP+" co, "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" mn" +
				" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\' and "+
				"co.pmid = mn.pmid";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		StringBuffer sb = new StringBuffer();
		while(rs.next()){
			sb.append(rs.getInt(1)).append(',');
		}
		rs.close();
		stmt.close();
		int span = MedlineAuthorAnalysis.getYearSpanFromYearString(sb.toString());
		return span;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		AuthorDSBDAOJdbcImpl authordsb = new AuthorDSBDAOJdbcImpl();
		String authority ="pavlovic_m_685713_4";
		int span = authordsb.getYearSpanByAuthorityId(authority);
		System.out.println(span);
	}

	@Override
	public void closeService() throws SQLException {
		articleDBConnection.close();
	}

	@Override
	public String getAuthorNameByAuthorityId(String authorityId) throws SQLException {
		List<String> lastNames = getAuthorLastNameByAuthorityId(authorityId);
		List<String> mis = getAuthorMiddleInitialByAuthorityId(authorityId);
		List<String> firstNames = getAuthorFirstNameByAuthorityId(authorityId);
		String lastName = "LASTNAME";
		String firstName = "FIRSTNAME";
		String mi = "MI";
		for(String name: lastNames){
			if(!name.equalsIgnoreCase("null")){
				lastName = name;
				break;
			}
		}
		
		for(String name: mis){
			if(!name.equalsIgnoreCase("null")){
				mi = name;
				break;
			}
		}
		
		for(String name: firstNames){
			if(!name.equalsIgnoreCase("null")){
				firstName = name;
				break;
			}
		}
		
		
		return lastName+", "+mi+", "+firstName;
	}

	@Override
	public List<String> getAuthorLastNameByAuthorityId(String authorityId) throws SQLException {
		
		String sql = "select nameVariation from LastName where authorID=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = authorityDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> lastNames = new ArrayList<String>();
		while(rs.next()){
			lastNames.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return lastNames;
	}

	@Override
	public List<String> getAuthorFirstNameByAuthorityId(String authorityId)throws SQLException {
		String sql = "select nameVariation from FirstName where authorID=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = authorityDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> firstNames = new ArrayList<String>();
		while(rs.next()){
			firstNames.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return firstNames;
	}

	@Override
	public List<String> getAuthorMiddleInitialByAuthorityId(String authorityId)throws SQLException {
		String sql = "select nameVariation from MiddleInitial where authorID=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = authorityDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> mis = new ArrayList<String>();
		while(rs.next()){
			mis.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		return mis;
	}

	@Override
	public float getClusteringCoefByAuthorityId(String authorityId) throws SQLException {
		String sql = "select coef from "+ITableNames.CLUSTERING_COEF+" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = articleDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		float coef = 0;
		if(rs.next()){
			coef = rs.getFloat(1);
		}
		return coef;
	}

	@Override
	public float getClusteringCoefByAuthorityIdBetweenYears(String authorityId, int start, int end) throws SQLException {
		List<String> coAuthors = getCoAuthorsByAuthorityIdBetweenYears(authorityId, start, end);
		Set<String> coAuthorSet = new HashSet<String>(coAuthors);
		int count = 0;
		for(String coAuthor: coAuthors){
			List<String> coCoAuthors = getCoAuthorsByAuthorityIdBetweenYears(coAuthor, start, end);
			for(String cocoAuthor: coCoAuthors){
				if(coAuthorSet.contains(cocoAuthor) && !cocoAuthor.equals(coAuthor))
					count++;
			}
		}
		// The changes in the following address the NaN value of coef. The previous program may not been updated.
		float coef = 0.0f ;// have no co-author at all
		
		if(coAuthors.size() >1)	// have multiple co-authors
			coef =  count* 1.0f/(coAuthors.size()*(coAuthors.size()-1)); // actually should divide by 2
		return coef;
	}
}
