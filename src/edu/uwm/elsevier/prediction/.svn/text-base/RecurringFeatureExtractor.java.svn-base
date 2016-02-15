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
import java.sql.SQLException;

import org.apache.lucene.queryParser.ParseException;

import edu.uwm.elsevier.authoranalysis.InterDisipline;

/**
 * @author qing
 *
 */
public class RecurringFeatureExtractor {

	private static int DEFAULT_YEAR = 2010;
	
	private SRWFeatureExtraction srwFeatureExtractor ;
	private AdvancedFeatureExtraction advFeatureExtractor;
	
	public RecurringFeatureExtractor() throws ClassNotFoundException, SQLException, IOException{
		srwFeatureExtractor = new SRWFeatureExtraction(DEFAULT_YEAR);
		InterDisipline interDisipline = new InterDisipline(DEFAULT_YEAR);
		srwFeatureExtractor.setInterDisipline(interDisipline);
		advFeatureExtractor = new AdvancedFeatureExtraction();
	}
	
	public FeatureEntity getFeatureForPair(AuthorshipEdge edge, int year) throws SQLException, ParseException, IOException{
		srwFeatureExtractor.setEndYear(year);
		srwFeatureExtractor.getInterDisipline().setEndYear(year);
		advFeatureExtractor.reset(edge.src, edge.dest, year);
		
		FeatureEntity feature = new FeatureEntity();
		int srcYearSpan = srwFeatureExtractor.getYearSpanOfAuthorBeforeYear(edge.src);
		int destYearSpan = srwFeatureExtractor.getYearSpanOfAuthorBeforeYear(edge.dest);
		feature.src = edge.src;
		feature.dest = edge.dest;
		feature.sumPub = srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.src)*1.0f/srcYearSpan
				+srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.dest)*1.0f/destYearSpan;
		if(feature.sumPub == 0.0f)
			return null;
		feature.simFulltex = srwFeatureExtractor.getCosineOfFullText(edge);
		feature.simMesh = srwFeatureExtractor.getCosineOfMesh(edge);
		feature.numCommonFriend = srwFeatureExtractor.getNumOfCommonFriends(edge);
		feature.sumCoAuthor = srwFeatureExtractor.getNumOfCoauthors(edge.src)*1.0f/srcYearSpan
				+srwFeatureExtractor.getNumOfCoauthors(edge.dest)*1.0f/destYearSpan;
		feature.sumClusteringCoef = srwFeatureExtractor.getClusteringCoef(edge.src) + srwFeatureExtractor.getClusteringCoef(edge.dest);
		feature.simIncite = srwFeatureExtractor.getInciteCosine(edge);
		feature.simOutcite = srwFeatureExtractor.getOutciteCosine(edge); //TODO enable when index is done.
		feature.recency = srwFeatureExtractor.getCumulatedRecency(edge);
		feature.positionSim = srwFeatureExtractor.getAuthorPositionSimilarity(edge);
		feature.instituteSimilarity = srwFeatureExtractor.getInstituteSimilarity(edge);
		feature.jaccard = advFeatureExtractor.getJaccardCoef();
		feature.adamic = advFeatureExtractor.getAdamicAdar();
		return feature;
	}
	
	public FeatureEntity getSimpleFeature(AuthorshipEdge edge, int startYear, int endYear) throws SQLException, ParseException, IOException{
		srwFeatureExtractor.setEndYear(endYear);
		srwFeatureExtractor.setStartYear(startYear);
		srwFeatureExtractor.getInterDisipline().setEndYear(endYear);
		srwFeatureExtractor.getInterDisipline().setStartYear(startYear);
//		advFeatureExtractor.reset(edge.src, edge.dest, startYear, endYear);
		
		FeatureEntity feature = new FeatureEntity();
		int srcYearSpan = srwFeatureExtractor.getYearSpanOfAuthorBeforeYear(edge.src);
		int destYearSpan = srwFeatureExtractor.getYearSpanOfAuthorBeforeYear(edge.dest);
		feature.src = edge.src;
		feature.dest = edge.dest;
		feature.sumPub = srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.src)*1.0f/srcYearSpan
				+srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.dest)*1.0f/destYearSpan;
		if(feature.sumPub == 0.0f)
			return null;
		feature.simFulltex = srwFeatureExtractor.getCosineOfFullText(edge);
		feature.simMesh = srwFeatureExtractor.getCosineOfMesh(edge);
		feature.numCommonFriend = srwFeatureExtractor.getNumOfCommonFriends(edge);
		feature.sumCoAuthor = srwFeatureExtractor.getNumOfCoauthors(edge.src)*1.0f/srcYearSpan
				+srwFeatureExtractor.getNumOfCoauthors(edge.dest)*1.0f/destYearSpan;
		feature.sumClusteringCoef = srwFeatureExtractor.getClusteringCoef(edge.src) + srwFeatureExtractor.getClusteringCoef(edge.dest);
		return feature;
	}
	
	
	public void processPairs(String path, String clsLabel) throws IOException, SQLException, ParseException, ClassNotFoundException{
		System.out.println("Start processing pairs in "+path);
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int count = 0;
		FileWriter writer = new FileWriter(path+".feature");
		  while ((strLine = br.readLine()) != null){
			  String[] fields = strLine.split("\t");
			  if(fields.length >=5 && fields[0].length() >0 && fields[1].length() >0 && fields[3].length() >0){
				  AuthorshipEdge edge = new AuthorshipEdge(fields[0], fields[1]);
				  int endYear = Integer.parseInt(fields[4]);	// it is possible that start year is the same as end year
				  if(clsLabel.equalsIgnoreCase("neg"))
					  endYear = 2010;
				  FeatureEntity feature = getFeatureForPair(edge, endYear);
				  if(feature != null){
					  writer.append(feature.toString()).append(",").append(clsLabel).append(',').append(fields[2]).append("\n");
					  count ++;
					  if(count % 10 == 0)
						  System.out.println(count+" pairs have been processed.");
				  }
			  }
		  }
		  writer.close();
	}
	
	public void processBaselinePairs(String path, String clsLabel, int year) throws IOException, SQLException, ParseException{
		System.out.println("Start processing baseline pairs in "+path);
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int count = 0;
		FileWriter writer = new FileWriter(path+".feature");
		  while ((strLine = br.readLine()) != null){
			  String[] fields = strLine.split(",");
			  if(fields.length >=2 && fields[0].length() >0 && fields[1].length() >0 ){
				  AuthorshipEdge edge = new AuthorshipEdge(fields[0], fields[1]);
				  FeatureEntity feature = getFeatureForPair(edge, year);
				  if(feature != null){
					  writer.append(feature.toString()).append(",").append(clsLabel).append("\n");
					  count ++;
					  if(count % 10 == 0)
						  System.out.println(count+" pairs have been processed.");
				  }
			  }
		  }
		  writer.close();
	}
	
	public void closeAllStuff() throws SQLException, IOException{
		srwFeatureExtractor.closeAllStuff();
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException {
		if(args.length <2){
			System.out.println("--pair-path --class-label");
			return;
		}
		RecurringFeatureExtractor extractor = new  RecurringFeatureExtractor();
		extractor.processPairs(args[0], args[1]);
		System.out.println("===Task Done.===");

	}

}
