/**
 * 
 */
package edu.uwm.elsevier.linkanalysis.matrix;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;

/**
 * @author qing
 *
 */
public class SparseMatrix {
	
	private AuthorDSBService authorDSBService;
	private CitationNetworkService citationNetworkService;
	private ArticlesDataDBConnection databaseConnection;
	private Logger logger = Logger.getLogger(SparseMatrix.class);

	public SparseMatrix() throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
		databaseConnection = ArticlesDataDBConnection.getInstance();
		citationNetworkService = new CitationNetworkService();
	}
	
	public void generateArticleVectorSpace(String save) throws SQLException, IOException{
		String sql = "select distinct pmid from "+ITableNames.AUTHORITY_MAP;
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		FileWriter writer = new FileWriter(save);
		while(rs.next()){
			writer.append(String.valueOf(rs.getLong(1))).append('\n');
		}
		rs.close();
		stmt.close();
		writer.close();
		logger.info("Task done.");
	}
	
	public void generateAuthorVectorSpace(String save) throws SQLException, IOException{
		String sql = "select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP;
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		FileWriter writer = new FileWriter(save);
		while(rs.next()){
			writer.append(rs.getString(1)).append('\n');
		}
		rs.close();
		stmt.close();
		writer.close();
		logger.info("Task done.");
	}
	
	public void generateArticleMatrix() throws SQLException{
		String sql = "select pmid, aac_article_index from "+ITableNames.AAC_ARTICLE;
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs= stmt.executeQuery(sql);
		while(rs.next()){
			int index = rs.getInt(2);
			List<Long> pmids = getDestPmidsBySrcPmid(rs.getLong(1));
			List<Integer> indices = getMatrixIndicesByPmids(pmids);
			insertArticleMatrixRow(index, indices);
		}
		rs.close();
		stmt.close();
	}
	
	public void generateAuthorMatrix() throws SQLException{
		String sql = "select authority_author_id, aac_author_index from "+ITableNames.AAC_AUTHOR;
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs= stmt.executeQuery(sql);
		while(rs.next()){
			String authorityId = rs.getString(1);
			int index = rs.getInt(2);
			List<Long> pmids = authorDSBService.getPmidsByAuthorityId(authorityId);
			List<Integer> indices = getMatrixIndicesByPmids(pmids);
			insertAuthorMatrixRow(index, indices);
		}
		rs.close();
		stmt.close();
	}
	
	public void insertArticleMatrixRow(int index, List<Integer> indices) throws SQLException{
		if(indices.size()==0)
			return;
		String sql ="insert into "+ITableNames.AAC_ARTICLE_MATRIX+"(src_index, dest_index) values (?, ?)";
		PreparedStatement prep = databaseConnection.getConnection().prepareStatement(sql);
		for(int target: indices){
			prep.setInt(1, index);
			prep.setInt(2, target);
			prep.addBatch();
		}
		prep.executeUpdate();
		prep.close();
	}
	
	public void insertAuthorMatrixRow(int index, List<Integer> indices) throws SQLException{
		if(indices.size() ==0)
			return;
		String sql = "insert into "+ITableNames.AAC_AUTHOR_MATRIX+"(author_index, article_index) values (?, ?)";
		PreparedStatement prep = databaseConnection.getConnection().prepareStatement(sql);
		for(int target: indices){
			prep.setInt(1, index);
			prep.setInt(2, target);
			prep.addBatch();
		}
		prep.executeUpdate();
		prep.close();
	}
	
	public List<Long> getDestPmidsBySrcPmid(long pmid) throws SQLException{
		List<Long> pmids = new ArrayList<Long>();
		List<Integer> articleIds = citationNetworkService.getArticleIdByPMID(pmid);
		if(articleIds.size() >1) // if has multi mapping, it is wrong. totally ignore this id..
			return pmids;
		String sql ="select dest_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+" where src_article_id=" + articleIds.get(0);
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			int articleId = rs.getInt(1);
			pmids.add(citationNetworkService.getPMIDByArticleId(articleId));
		}
		return pmids;
	}
	
	public List<Integer> getMatrixIndicesByPmids(List<Long> pmids) throws SQLException{
		List<Integer> indices = new ArrayList<Integer>();
		if(pmids.size() ==0)
			return indices;
		String sql =" select aac_article_index from "+ITableNames.AAC_ARTICLE+" where pmid=?";
		PreparedStatement prep = databaseConnection.getConnection().prepareStatement(sql);
		for(Long pmid: pmids){
			prep.setLong(1, pmid);
			prep.addBatch();
		}
		ResultSet rs = prep.executeQuery();
		
		while(rs.next()){
			indices.add(rs.getInt(1));
		}
		prep.close();
		rs.close();
		return indices;
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		SparseMatrix matrix = new SparseMatrix();
		matrix.generateAuthorVectorSpace(args[0]+"authority.csv");
		matrix.generateArticleVectorSpace(args[0]+"pmids.csv");
	}

}
