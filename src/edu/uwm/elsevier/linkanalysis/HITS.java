/**
 * 
 */
package edu.uwm.elsevier.linkanalysis;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.uwm.elsevier.ITableNames;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class HITS {
	
	private float[] hubs;
	private float[] auths;
	private int dim = 1340519;
//	private int dim = 1000;
	public static float hitsConvergeThreshold =  1e-6f;
	private List<List<Integer>> srcByDest;
	private List<List<Integer>> destBySrc;
//	private String saveRoot = "/Users/qing/";
	private String saveRoot = "/home/qzhang/";
	
	private ArticlesDataDBConnection databaseConnection;
	private Logger logger = Logger.getLogger(HITS.class);
	public HITS() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	private void initializeHubsAuths(){
		hubs = new float[dim];
		auths = new float[dim];
		for(int i=0; i<dim; i++){
			hubs[i] = 1.0f;
			auths[i] = 1.0f;
		}
	}
	/**
	 * HITS Algorithm
	 * @throws SQLException
	 * @throws IOException 
	 */
	public void getHubsAndAuthorities() throws SQLException, IOException{
		
		initializeHubsAuths();
		loadMatrices();
//		testSetUp();
		
		boolean hasConverged = false;
		float norm = 0.0f;
		float[] prevAuths =new float[dim];
		float[] prevHubs = new float[dim];
		int iter = 0;
		float diff = 10.0f;
		float authDiff = 10.0f;
		float hubDiff = 10.0f;
		while(!hasConverged){
			iter++;
			logger.info("In iteration "+iter);
			for(int i=0; i<dim; i++){
				auths[i] = 0.0f;
//				List<Integer> incites = getArticleIndexByMatrixIndex(i, ITableNames.MEDLINE_MATRIX_DEST_BY_SRC);
				List<Integer> incites = destBySrc.get(i);
				for(Integer incite: incites){
					auths[i] += hubs[incite];
				}
				norm += auths[i] * auths[i];
			}
			
			norm = (float)Math.sqrt(norm);
			for(int i=0; i<dim; i++){
				auths[i] = auths[i]/norm;
			}

			norm = 0.0f;
			for(int i=0; i<dim; i++){
				hubs[i] = 0.0f;
//				List<Integer> outcites = getArticleIndexByMatrixIndex(i, ITableNames.MEDLINE_MATRIX_SRC_BY_DEST);
				List<Integer> outcites = srcByDest.get(i);
				for(Integer outcite: outcites){
					hubs[i] += auths[outcite];
				}
				norm += hubs[i] * hubs[i];
			}
			
			norm = (float)Math.sqrt(norm);
			for(int i=0; i<dim; i++){
				hubs[i] = hubs[i]/norm;
			}
			
			authDiff = getDiff(prevAuths, auths);
			hubDiff = getDiff(prevHubs, hubs);
			System.arraycopy(auths, 0, prevAuths, 0, dim);
			System.arraycopy(hubs, 0, prevHubs, 0, dim);
			diff = Math.max(authDiff, hubDiff);
			logger.info("diff="+diff);
			hasConverged = (diff < hitsConvergeThreshold);
		}
		logger.info("Iteration done.");
		
		saveVector(saveRoot+"auths.txt", auths);
		saveVector(saveRoot+"hubs.txt", hubs);
		
	}
	
	public void testSetUp(){
		dim=6;
		initializeHubsAuths();
		int[][] graph = new int[][]{
				{0,1,0,0,0,0},
				{0,0,0,0,1,0},
				{0,1,0,0,0,0},
				{0,1,1,0,0,0},
				{0,0,0,0,0,0},
				{0,1,0,0,0,0}};
		
		srcByDest = new ArrayList<List<Integer>>(dim);
		destBySrc = new ArrayList<List<Integer>>(dim);
		
		List<Integer> outcites  ;
		List<Integer> incites;
		for(int i=0; i<dim; i++){
			outcites = new ArrayList<Integer>();
			for(int j=0; j<dim; j++){
				if(graph[i][j] !=0)
					outcites.add(graph[i][j]);
			}
			srcByDest.add(outcites);
		}
		for(int j=0; j<dim; j++){
			incites = new ArrayList<Integer>();
			for(int i=0; i<dim; i++){
				if(graph[i][j]!=0)
					incites.add(graph[i][j]);
			}
			destBySrc.add(incites);
		}
		
	}
	
	public void print(){
		for(int i=0; i<dim; i++){
			System.out.println(auths[i]+"\t"+hubs[i]);
		}
	}
	
	public void loadMatrices() throws SQLException{
		logger.info("Loading matrices ...");
		long start = System.currentTimeMillis();
		srcByDest = new ArrayList<List<Integer>>(dim);
		destBySrc = new ArrayList<List<Integer>>(dim);
		List<Integer> outcites = null;
		List<Integer> incites = null;
		for(int i=0; i<dim; i++){
//			long start = System.currentTimeMillis();
			outcites = getArticleIndexByMatrixIndex(i, ITableNames.MEDLINE_MATRIX_SRC_BY_DEST); 
//			long end = System.currentTimeMillis();
//			logger.info("getting single row: "+(end-start)+" ms");
			incites = getArticleIndexByMatrixIndex(i, ITableNames.MEDLINE_MATRIX_DEST_BY_SRC);
			srcByDest.add(outcites);
			destBySrc.add(incites);
//			logger.info("loop: "+(loopend-start)+ " ms");
			if(i%1000==0)
				logger.info(i+" rows loaded.");
		}
		long total = System.currentTimeMillis() -start;
		logger.info("Matrices loaded. time:"+total+" ms.");
	}
	
	public void saveVector(String save, float[] vec) throws IOException{
		logger.info("Writing vec to "+save);
		FileWriter writer = new FileWriter(save);
		for(int i=0; i<dim; i++){
			writer.append(String.valueOf(i)).append('\t').append(String.valueOf(vec[i])).append('\n');
		}
		writer.close();
	}
	
	public float getDiff(float[] prev, float[] cur){
		if(prev==null)
			return 10.0f;
		float max = 0f;
		for(int i=0; i<dim; i++){
			float d = Math.abs(prev[i] - cur[i]);
			if(d > max)
				max = d;
		}
		return max;
	}
	
	public List<Integer> getArticleIndexByMatrixIndex(int index, String table) throws SQLException{
		String sql = "select node_article_ids from "+table+" where matrix_index="+index;
		List<Integer> articleIndices = new ArrayList<Integer>();
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		String idStr = null;
		if(rs.next()){
			idStr = rs.getString(1);
		}
		rs.close();
		
			
		List<Integer> ids = parseIds(idStr);
		if(ids.size() == 0){
//			logger.info("id.size =0 for index:"+index+"\ttable:"+table);
			return articleIndices;
		}
		sql = "select matrix_index from "+table+" where article_id in "+getInclouse(ids);
		rs = stmt.executeQuery(sql);
		while(rs.next()){
			articleIndices.add(rs.getInt(1));
		}
		rs.close();
		return articleIndices;
	}
	
	public String getInclouse(List<Integer> ids){
		StringBuffer sb = new StringBuffer();
		boolean isFirst = true;
		sb.append("(");
		for(Integer articleId: ids){
			if(isFirst){
				sb.append(articleId);
				isFirst = false;
			}else{
				sb.append(",").append(articleId);
			}
		}
		sb.append(")");
		return sb.toString();
	}
	
	public List<Integer> parseIds(String idStr){
		List<Integer> articleIds = new ArrayList<Integer>();
		if(idStr==null)
			return articleIds;
		String[] ids = idStr.split(",");
		for(String id: ids){
			if(id.length() >0)
				articleIds.add(Integer.parseInt(id));
		}
		return articleIds;
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		HITS hits = new HITS();
		try {
			hits.getHubsAndAuthorities();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
