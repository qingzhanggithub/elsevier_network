/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.authoranalysis.InterDisipline;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.namedisambiguation.AuthorityTool;

/**
 * @author qing
 *
 */
public class CoAuthorshipExtractor {

	private ArticlesDataDBConnection databaseConnection;
	private Logger logger = Logger.getLogger(CoAuthorshipExtractor.class);
	public CoAuthorshipExtractor() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	public List<AuthorshipEdge> getCoAuthorshipsByYear(int startYear, int endYear, int size) throws SQLException{
		System.out.println("Getting authorships by year. size wanted: "+size);
		String sql = "select authority_author_id , co_author_authority_author_id, pmid, year from "+ITableNames.CO_AUTHOR+
				" where year >="+startYear+" and year <"+endYear+
				" and strcmp(authority_author_id, co_author_authority_author_id)=-1";
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		List<AuthorshipEdge> edges = new ArrayList<AuthorshipEdge>();
		int count = 0;
		while(rs.next() && count <size){
			AuthorshipEdge edge  = new AuthorshipEdge(rs.getString(1), rs.getString(2));
			List<PublicationAttribute> publications = new ArrayList<PublicationAttribute>();
			PublicationAttribute publication = new PublicationAttribute(rs.getLong(3), rs.getInt(4));
			publications.add(publication);
			edge.publications = publications;
			edges.add(edge);
			count++;
			if(count % 500 == 0){
				System.out.println(count+" edges extracted ..");
			}
		}
		rs.close();
		stmt.close();
		return edges;
	}
	
	public List<AuthorshipEdge> getNegativeAuthorPairs(int startYear, int endYear, int size) throws SQLException{
		System.out.println("Getting negtive author pairs with size: "+size);
		String sql ="select distinct authority_author_id, med.year from "+ITableNames.AUTHORITY_MAP+" am, "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" med where am.pmid = med.pmid  and med.year <"+startYear+" order by rand()";
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		int count = 0;
		List<AuthorshipEdge> negEdges = new ArrayList<AuthorshipEdge>();
		while(rs.next()){
			String src = rs.getString(1);
			if(rs.next()){
				String dest = rs.getString(1);
				if(!everCoauthor(src, dest, -1, endYear)){
					AuthorshipEdge edge = new AuthorshipEdge(src, dest);
					negEdges.add(edge);
					count ++;
//					System.out.println("found "+count+" neg pairs");
					if(count % 100 == 0)
						System.out.println(count+" neg pairs has been found");
					if(count == size)
						break;
				}
			}else
				break;
		}
		rs.close();
		return negEdges;
	}
	
	private int findEarlistYear(String str){
		int earlist = 2012;
		String[] years = str.split(",");
		for(int i=0; i<years.length; i++){
			if(years[i].length() >0){
				int y = Integer.parseInt(years[i]); 
				if(y < earlist)
					earlist = y;
			}
		}
		return earlist;
	}
	
	public boolean everCoauthor(String src, String dest, int start, int end) throws SQLException{
		String sql = "select * from "+ITableNames.CO_AUTHOR+
				" where authority_author_id=\'"+AuthorityTool.escape(src)+"\' and co_author_authority_author_id=\'"+AuthorityTool.escape(dest)+"\'";
		if(start != -1)
			sql += " and year >= "+ start;
		if(end != -1)
			sql += " and year < "+end;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		boolean flag = rs.next();
		rs.close();
		stmt.close();
		return flag;
	}
	
	public void assignPosNeg(String path, int trainingEndYear, int testEndYear, int posSize, int negSize) throws IOException, SQLException{
		FileInputStream fstream = new FileInputStream(path);
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  FileWriter writer = new FileWriter(path+posSize+"_"+negSize+".labelled");
		  int posCount =0;
		  int negCount =0;
		  boolean posDone = false;
		  boolean negDone = false;
		  int total =0;
		  while ((strLine = br.readLine()) != null)   {
			  
			  String[] fields = strLine.split("\t");
			  if(fields.length >=2 && fields[0].length()>0 && fields[1].length() >0){
				  boolean trainFlag = everCoauthor(fields[0], fields[1], -1, trainingEndYear);
				  boolean testFlag = everCoauthor(fields[0], fields[1], trainingEndYear, testEndYear);
				  if(trainFlag){
					  if(testFlag && !posDone){
						  writer.append(strLine).append("\tpos\n");
						  posCount++;
						  if(posCount %10 ==0)
							  System.out.println("pos count: "+posCount);
						  if(posCount == posSize)
							  posDone = true;
					  }
					  else if (!testFlag && !negDone){
						  writer.append(strLine).append("\tneg\n");
						  negCount ++;
						  if(negCount %10 ==0)
							  System.out.println("neg count: "+negCount);
						  if(negCount  == negSize)
							  negDone = true;
					  }
				  }
			  }
		  }
		  total ++;
		  if(total % 100 ==0)
			  System.out.println(total+" lines processed.");
		  writer.close();
		  System.out.println("Labeller task done.");
	}
	
	public void extractFeatures(int start, int end, int total, String save) throws IOException, ParseException, SQLException, ClassNotFoundException{
		FileWriter writer = new FileWriter(save);
		List<AuthorshipEdge> trainingEdges = getCoAuthorshipsByYear(start, end, total);
		SRWFeatureExtraction srwFeatureExtractor = new SRWFeatureExtraction(start);
		srwFeatureExtractor.setTrainingEdges(trainingEdges);
		InterDisipline interDisipline = new InterDisipline(start);
		srwFeatureExtractor.setInterDisipline(interDisipline);
		int i=0;
		int size = trainingEdges.size();
		writer.append("src,dest,sum_pub,mesh_cos,fulltext_cos,common_friends,sum_coauthor,clustering_coef,connect").append("\n");
		for(AuthorshipEdge edge: trainingEdges){
			float simFullText = srwFeatureExtractor.getCosineOfFullText(edge);
			writer.append(edge.src).append(",").append(edge.dest).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.src)+srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.dest))).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getCosineOfMesh(edge))).append(",");
			writer.append(String.valueOf(simFullText)).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getNumOfCommonFriends(edge))).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getNumOfCoauthors(edge.src)+ srwFeatureExtractor.getNumOfCoauthors(edge.dest))).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getClusteringCoef(edge.src) + srwFeatureExtractor.getClusteringCoef(edge.dest))).append(',');
			writer.append("pos").append("\n");
			i++;
			if(i %500 == 0){
				logger.info(i+"/"+size+" edge features have been processed.");
			}
			
		}
		writer.close();
		logger.info("Finish extracting features .");
	}
	
	public void extractNegFeatures(int start, int end, int total, String save) throws IOException, SQLException, ClassNotFoundException, ParseException{
		FileWriter writer = new FileWriter(save);
		List<AuthorshipEdge> negs = getNegativeAuthorPairs(start, end, total);
		SRWFeatureExtraction srwFeatureExtractor = new SRWFeatureExtraction(start);
		srwFeatureExtractor.setTrainingEdges(negs);
		InterDisipline interDisipline = new InterDisipline(start);
		srwFeatureExtractor.setInterDisipline(interDisipline);
		int i=0;
		int size = negs.size();
		writer.append("src,dest,sum_pub,mesh_cos,fulltext_cos,common_friends,sum_coauthor,clustering_coef,connect").append("\n");
		for(AuthorshipEdge edge: negs){
			float simFullText = srwFeatureExtractor.getCosineOfFullText(edge);
			writer.append(edge.src).append(",").append(edge.dest).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.src)+srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.dest))).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getCosineOfMesh(edge))).append(",");
			writer.append(String.valueOf(simFullText)).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getNumOfCommonFriends(edge))).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getNumOfCoauthors(edge.src)+srwFeatureExtractor.getNumOfCoauthors(edge.dest))).append(",");
			writer.append(String.valueOf(srwFeatureExtractor.getClusteringCoef(edge.src) + srwFeatureExtractor.getClusteringCoef(edge.dest))).append(',');
			writer.append("neg").append("\n");
			i++;
			if(i % 500 == 0){
				logger.info(i+" / "+ size+" neg edges have been processed.");
			}
		}
		writer.close();
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws ParseException 
	 * @throws IOException 
	 * @throws NumberFormatException
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NumberFormatException, IOException, ParseException {
		if(args.length != 5){
			System.out.println("--start-year --end-year --total-wanted --save --type (pos / neg)");
			return;
		}
		CoAuthorshipExtractor extractor = new CoAuthorshipExtractor();
		if(args[4].equalsIgnoreCase("pos"))
			extractor.extractFeatures(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
		else
			extractor.extractNegFeatures(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
	}

}
