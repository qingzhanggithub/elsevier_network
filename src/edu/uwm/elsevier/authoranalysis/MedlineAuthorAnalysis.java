/**
 * 
 */
package edu.uwm.elsevier.authoranalysis;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.csvreader.CsvReader;


import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.analysis.MeshAnalysis;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;

/**
 * @author qing
 *
 */
public class MedlineAuthorAnalysis {
	
	private AuthorDSBService authorDSBService;
	private ArticlesDataDBConnection articledbConnection;
	private CitationNetworkService citationNetworkService;
	private Logger logger = Logger.getLogger(MedlineAuthorAnalysis.class);
	private String insertRecord = "insert into "+ITableNames.AUTHOR_YEARS+" (authority_author_id, mincite, mincite_years, eincite, eincite_years, myear_span, eyear_span, author_span) values (?, ?, ?, ?, ?,?,?,?)";
	private PreparedStatement prepInsert;
	private String insertComponent = "insert into "+ITableNames.CO_AUTHOR_CONNECTED_COMPONENT+" (authority_author_id, component_num) values(?, ?)";
	private PreparedStatement prepInsertComp;
	
	public MedlineAuthorAnalysis() throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
		articledbConnection = ArticlesDataDBConnection.getInstance();
		prepInsert = articledbConnection.getConnection().prepareStatement(insertRecord);
		citationNetworkService = new CitationNetworkService();
	}
	
	public float getClusteringCoefficient(String authorityId) throws SQLException{
		List<String> coAuthors = authorDSBService.getCoAuthorsByAuthorityId(authorityId);
		int degree = coAuthors.size();
		if(degree == 0) // clustering coef desn't apply for unconnected node.
			return  -1.0f;
		int edges = getEdges(coAuthors);
		float coef = 0.0f;
		if(degree >0){
			int k = 1;
			if(degree >1)
				k = degree*(degree-1);
			coef = edges*2.0f/k;
//			if(edges!=0)
//				logger.info(authorityId+":edges="+edges+"\tdegree="+degree+"\tcoef="+coef);
		}
//		else
//			logger.info(authorityId+":degree==0");
		return coef;
	}
	
	public void setLoggingLevel(String level){
		if(level.equals("info"))
			logger.setLevel(Level.INFO);
		else if(level.equals("debug"))
			logger.setLevel(Level.DEBUG);
		else if(level.equals("error"))
			logger.setLevel(Level.ERROR);
	}
	
	public void getClusteringCoefficients(String save) throws SQLException, IOException{
		logger.info("Task start. calculating clustering coef ...");
		String sql ="select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP;
		Statement stmt = articledbConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		FileWriter writer = new FileWriter(save);
		int count =0;
		while(rs.next()){
			count++;
			String authorityId = rs.getString(1);
			float coef = getClusteringCoefficient(authorityId);
			if(coef <0) // isolated node, ignore.
				continue;
			writer.append(authorityId).append('\t').append(String.valueOf(coef)).append('\n');
			if(count % 1000 == 0)
				logger.info("count="+count);
		}
		writer.close();
		rs.close();
		stmt.close();
		logger.info("Task done.");
	}
	
	public void getInicteYearForAuthors(String save, int start) throws SQLException, ClassNotFoundException, IOException{
		logger.info("Task start. Getting year information for authors ...");
		String sql ="select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP;
		Statement stmt = articledbConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
//		FileWriter writer = new FileWriter(save);
		int count = 0;
		while(rs.next()){
			count++;
			if(count < start)
				continue;
			String authorityId = rs.getString(1);
			int incites = 0;
			int eincites = 0;
			List<Integer> articleIds = authorDSBService.getArticleIdsByAuthorityId(authorityId);
			StringBuffer sb = new StringBuffer();
			StringBuffer einciteYearSb = new StringBuffer();
			for(Integer articleId: articleIds){
				String minciteyears = MeshAnalysis.getMinciteYears(articleId);
				sb.append(minciteyears);
				einciteYearSb.append(MeshAnalysis.getEinciteYears(articleId));
				MedlineAritcleNodeStatistiscs stats = citationNetworkService.getMedlineAritcleNodeStatistiscsByArticleId(articleId);
				if(stats != null){
					incites += stats.getIncitesFromMedline();
					eincites += stats.getIncites();
				}
			}
			String mYearStr = sb.toString(); // NOTE: it is incite years!! not author publish years!!
			String eYearStr = einciteYearSb.toString();
			int minciteYearSpan = getYearSpanFromYearString(mYearStr);
			int einciteYearSpan = getYearSpanFromYearString(eYearStr);
			int authorYearSpan = authorDSBService.getYearSpanByAuthorityId(authorityId);
			insert(authorityId, incites, mYearStr, eincites, eYearStr, minciteYearSpan, einciteYearSpan, authorYearSpan);
			if(count %1000 ==0){
				prepInsert.executeBatch();
				logger.info("count="+count);
			}
		}
		prepInsert.executeBatch();
		prepInsert.close();
//		writer.close();
		rs.close();
		stmt.close();
		logger.info("Task done.");
	}
	
	public void getAuthorYearSpan(String save) throws SQLException, IOException{
		logger.info("Task start. Getting author year span ...");
		String sql ="select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP;
		Statement stmt = articledbConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		int count = 0;
		FileWriter writer = new FileWriter(save);
		while(rs.next()){
			count++;
			String authorityId = rs.getString(1);
			int authorYearSpan = authorDSBService.getYearSpanByAuthorityId(authorityId);
			writer.append(authorityId).append('\t').append(String.valueOf(authorYearSpan)).append('\n');
			if(count %1000 ==0)
				logger.info("count:"+count);
		}
		writer.close();
		rs.close();
		stmt.close();
		logger.info("Task done.");
	}
	
	public void analyzeCoAuthorshipTimeSpan(String path, String save) throws IOException{
		FileWriter writer = new FileWriter(save);
		CsvReader reader = new CsvReader(path, '\t');
		int count =0;
		while(reader.readRecord()){
			count++;
			String src = reader.get(0);
			String dest = reader.get(1);
			String yearsStr = reader.get(2);
			int span = getYearSpanFromYearString(yearsStr);
			writer.append(src).append('\t').append(dest).append('\t').append(String.valueOf(span)).append('\n');
			if(count % 1000 == 0)
				logger.info("count="+count);
		}
		writer.close();
		logger.info("Task done.");
	}
	
	public void analyzeCoAuthorshipTimeSpanNormalized(String path, String save, int spanFrom) throws IOException, SQLException{
		FileWriter writer = new FileWriter(save);
		CsvReader reader = new CsvReader(path, '\t');
		int count =0;
		while(reader.readRecord()){
			count++;
			if(count % 1000 == 0)
				logger.info("count="+count);
			String src = reader.get(0);
			String dest = reader.get(1);
			String yearsStr = reader.get(2);
			int span = getYearSpanFromYearString(yearsStr);
			if(span < spanFrom) // exclude ones that are not interested .
				continue;
			int	srcSpan = authorDSBService.getYearSpanByAuthorityId(src);
			int destSpan = authorDSBService.getYearSpanByAuthorityId(dest);
			int avgSpan = (srcSpan+destSpan)/2;
			if(srcSpan==-1 && destSpan!=-1)
				avgSpan = destSpan;
			else if(srcSpan !=-1 && destSpan ==-1)
				avgSpan = srcSpan;
			float norm = 0;
			if(avgSpan >0){
				norm = span*1.0f/avgSpan;
			}
			writer.append(src).append('\t').append(dest).append('\t').append(String.valueOf(norm)).append('\n');
			
		}
		writer.close();
		logger.info("Task done.");
	}
	
	
	
	public static int getYearSpanFromYearString(String yearsStr){
		int span = -1;
		if(yearsStr !=null){
			String[] years = yearsStr.split(",");
			int min =2012;
			int max = 0;
			for(String year: years){
				if(year.length()==0)
					continue;
				int y = Integer.parseInt(year);
				if(y > max)
					max = y;
				if(y < min)
					min = y;
			}
			span = max - min+1; // 5-22-2012 need to add one. year 2000, 2001, 2002 are three years instead of two.
		}
		return span;
	}
	
	public void analyzeAuthorOverYear(String save) throws SQLException, IOException{
		String sql =" select count(distinct authority_author_id) from "+ITableNames.AUTHOR_YEARS;
		Statement stmt = articledbConnection.getConnection().createStatement();
		ResultSet rs ;
		FileWriter writer = new FileWriter(save);
		for(int i=1950; i<2012; i++){
			logger.info("Year:"+i);
			rs = stmt.executeQuery(sql+" where years like \'%"+i+"%\'");
			if(rs.next()){
				writer.append(String.valueOf(i)).append('\t').append(String.valueOf(rs.getInt(1))).append('\n');
			}
			rs.close();
		}
		writer.close();
	}
	
	public void analyzeAuthorOverYearNormalized(String save) throws SQLException, IOException{
		FileWriter writer = new FileWriter(save);
		for(int i=1950; i<2012; i++){
			logger.info("Year:"+i);
			int authorCount  =citationNetworkService.getMAuthorCountByYear(i);
			int yearCount = citationNetworkService.getMArticleCountByYear(i);
			float count = 0.0f;
			if(yearCount !=0)
				count = authorCount* 1.0f / yearCount;
			writer.append(String.valueOf(i)).append('\t').append(String.valueOf(count)).append('\n');
		}
		writer.close();
	}
	
	public void analyzeConnectedComponents(int begin) throws SQLException{
		logger.info("Task start. Detecting connected components ...");
		prepInsertComp = articledbConnection.getConnection().prepareStatement(insertComponent);
		int componentNum = begin;
		while(true){
			String start = next();
			if(start ==null)
				break; //all the nodes have been processed
			componentNum++;
			findComponentBFS(start, componentNum);
		}
		prepInsertComp.close();
		logger.info("Task done.");
	}
	
	public void findComponentBFS(String start, int componentNum) throws SQLException{
		logger.info("Finding elements for component "+componentNum);
		Queue quene = new Queue();
		quene.enquene(start);
		int count = 0;
		HashSet<String> discovered = new HashSet<String>();
		while(!quene.isEmpty()){
			count ++;
			String ele = quene.dequeue();
			insertComponent(ele, componentNum);
			List<String> coAuthors = authorDSBService.getCoAuthorsByAuthorityId(ele);
			for(String co: coAuthors){
				if(!discovered.contains(co)){
					quene.enquene(co);
					discovered.add(co);
				}
			}
			if(count% 1000 ==0){
				logger.info(count+" in component "+componentNum);
			}
		}
		logger.info("Total "+count+" elements found for component "+componentNum);
	}
	
	public void insertComponent(String authorityId, int componentNum) throws SQLException{
		if(prepInsertComp.isClosed()){
			prepInsertComp = articledbConnection.getConnection().prepareStatement(insertComponent);
		}
		prepInsertComp.setString(1, authorityId);
		prepInsertComp.setInt(2, componentNum);
		prepInsertComp.executeUpdate();
	}
	/**
	 * For the BFS of finding connected components only.
	 * @return
	 * @throws SQLException 
	 */
	private String next() throws SQLException{
		String sql = "select authority_author_id from "+ITableNames.CO_AUTHOR +
				" where authority_author_id not in (select authority_author_id from "+ITableNames.CO_AUTHOR_CONNECTED_COMPONENT+") limit 1";
		Statement stmt = articledbConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		if(rs.next()){
			return rs.getString(1);
		}else
			return null;
	}
	
	class Queue{
		List<String> list  = new ArrayList<String>();
		public void enquene(String ele){
			list.add(ele);
		}
		
		public String dequeue(){
			if(list.size() ==0)
				return null;
			return list.remove(0);
		}
		
		public void enqueueAll(List<String> eles){
			list.addAll(eles);
		}
		
		public boolean isEmpty(){
			return list.size() ==0;
		}
		
	}
	
	public void insert(String authority_author_id, //1
			int mincites, //2
			String mYears, //3
			int eincites, //4
			String eYears, //5
			int mYearSpan, //6
			int eYearSpan,//7
			int authorSpan) throws SQLException{ //8
		prepInsert.setString(1, authority_author_id);
		prepInsert.setInt(2, mincites);
		prepInsert.setString(3, mYears);
		prepInsert.setInt(4, eincites);
		prepInsert.setString(5, eYears);
		prepInsert.setInt(6, mYearSpan);
		prepInsert.setInt(7, eYearSpan);
		prepInsert.setInt(8, authorSpan);
		prepInsert.addBatch();
	}
	
	public int getEdges(List<String> coAuthors) throws SQLException{
		int edge = 0;
		int size = coAuthors.size();
		for(int i=0; i<size; i++){
			String author = coAuthors.get(i);
			for(int j=i+1; j<size; j++){
				String co = coAuthors.get(j);
				if(authorDSBService.isCoAuthor(author, co))
					edge++;
			}
		}
		return edge;
	}
	
	public void closeAllStuff() throws SQLException{
		if(articledbConnection != null){
			articledbConnection.close();
		}
	}
	
	public static void main(String[] args){
		if(args.length != 2){
			System.out.println("--save --start");
			return;
		}
		try {
			MedlineAuthorAnalysis ma = new MedlineAuthorAnalysis();
			ma.setLoggingLevel("info");
			ma.getClusteringCoefficients(args[0]);
//			ma.getInicteYearForAuthors(args[0], Integer.parseInt(args[1]));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
