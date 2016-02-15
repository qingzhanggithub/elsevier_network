/**
 * 
 */
package edu.uwm.elsevier.linkanalysis.matrix;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;


import edu.uwm.elsevier.ITableNames;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class AdjacentMatrix {

	private ArticlesDataDBConnection dbConnection ;
	private List<Integer> articleIds = null;
	private HashMap<Integer, Integer> articleIdMap = null;
	private Logger logger = Logger.getLogger(AdjacentMatrix.class);
	public static String ADJACENT_MATRIX_TYPE_SRC_BY_DEST = "src_by_dest";
	public static String ADJACENT_MATRIX_TYPE_DEST_BY_SRC = "dest_by_src";
	
	public AdjacentMatrix() throws ClassNotFoundException, SQLException{
		dbConnection = ArticlesDataDBConnection.getInstance();
	}
	
	
	public void createMatrix(String save, String matrixType) throws SQLException, IOException{
		logger.info("Creating sparse matrix.  matrixType="+matrixType);
		getIds();
//		int size = articleIds.size();
		int size = 1000;
		String sql =null;
		if(matrixType.equals(ADJACENT_MATRIX_TYPE_SRC_BY_DEST)){
			sql ="select dest_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+" where src_article_id=";
		}else
			sql ="select src_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+" where dest_article_id=";
		 java.sql.Statement stmt = dbConnection.getConnection().createStatement();
		 ResultSet rs = null;
		 FileWriter writer = new FileWriter(save);
		 logger.info("Writing rows . size="+size);
		for(int i=0; i<size; i++){
			int articleId = articleIds.get(i);
			rs = stmt.executeQuery(sql+articleId);
			StringBuffer sb = new StringBuffer();
			sb.append(i).append('\t');
			sb.append(articleId).append('\t');
			while(rs.next()){
				sb.append(rs.getInt(1)).append(',');
			}
			sb.append('\n');
			writer.append(sb.toString());
			rs.close();
			if(i%1000==0)
				logger.info("i="+i+"/"+size);
		}
		writer.close();
		logger.info("Task done. save to: "+save);
	}
	
	public void createFilteredMatrix(String save, String matrixType, Set<Integer> space) throws SQLException, IOException{
		if(articleIds ==null){
			articleIds = new ArrayList<Integer>();
		}else
			articleIds.clear();
		for(Integer id: space){
			articleIds.add(id);
		}
		int size = space.size();
		String sql =null;
		if(matrixType.equals(ADJACENT_MATRIX_TYPE_SRC_BY_DEST)){
			sql ="select dest_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+" where src_article_id=";
		}else
			sql ="select src_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+" where dest_article_id=";
		 java.sql.Statement stmt = dbConnection.getConnection().createStatement();
		 ResultSet rs = null;
		 FileWriter writer = new FileWriter(save);
		 logger.info("Writing rows . size="+size);
		for(int i=0; i<size; i++){
			int articleId = articleIds.get(i);
			rs = stmt.executeQuery(sql+articleId);
			StringBuffer sb = new StringBuffer();
			sb.append(i).append('\t');
			sb.append(articleId).append('\t');
			while(rs.next()){
				int target = rs.getInt(1);
				if(space.contains(target))
					sb.append(target).append(',');
			}
			sb.append('\n');
			writer.append(sb.toString());
			rs.close();
			if(i%1000==0)
				logger.info("i="+i+"/"+size);
		}
		writer.close();
		logger.info("Task done. save to: "+save);
	}
	
	public void getIds() throws SQLException{
		logger.info("Reading article_ids from medline network...");
		articleIdMap = new HashMap<Integer, Integer>();
		String sql = "(select distinct src_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+") union (select distinct dest_article_id from "+ITableNames.MEDLINE_NETWORK_TABLE+" )";
		java.sql.Statement stmt = dbConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		articleIds = new ArrayList<Integer>();
		while(rs.next()){
			articleIds.add(rs.getInt(1));
		}
		rs.close();
		stmt.close();
	}
	
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		if(args.length ==0){
			System.out.println("--save --matrix-type(optional)");
			return;
		}
		AdjacentMatrix matrix = new AdjacentMatrix();
		if(args.length ==2)
			matrix.createMatrix(args[0], args[1]);
		else{
			matrix.createMatrix(args[0]+"src_by_dest.txt", ADJACENT_MATRIX_TYPE_SRC_BY_DEST);
			matrix.createMatrix(args[0]+"dest_by_src.txt", ADJACENT_MATRIX_TYPE_DEST_BY_SRC);
		}
	}

}
