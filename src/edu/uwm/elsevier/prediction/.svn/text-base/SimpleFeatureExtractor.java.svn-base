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

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import edu.uwm.elsevier.authoranalysis.InterDisipline;

/**
 * @author qing
 *
 */
public class SimpleFeatureExtractor {
	
	private SRWFeatureExtraction srwFeatureExtractor ;
	private AdvancedFeatureExtraction advFeatureExtractor;
	public static int DEFAULT_YEAR  = 0;
	private Logger logger = Logger.getLogger(SimpleFeatureExtractor.class);
	
	public SimpleFeatureExtractor() throws ClassNotFoundException, SQLException, IOException{
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
		feature.simFulltex = srwFeatureExtractor.getCosineOfFullText(edge);
		feature.sumPub = srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.src)*1.0f/srcYearSpan+srwFeatureExtractor.getNumOfArticlesPublishedBeforeYear(edge.dest)*1.0f/destYearSpan;
		feature.simMesh = srwFeatureExtractor.getCosineOfMesh(edge);
		feature.numCommonFriend = srwFeatureExtractor.getNumOfCommonFriends(edge);
		feature.sumCoAuthor = srwFeatureExtractor.getNumOfCoauthors(edge.src)*1.0f/srcYearSpan+srwFeatureExtractor.getNumOfCoauthors(edge.dest)*1.0f/destYearSpan;
		feature.sumClusteringCoef = srwFeatureExtractor.getClusteringCoef(edge.src) + srwFeatureExtractor.getClusteringCoef(edge.dest);
		feature.simIncite = srwFeatureExtractor.getInciteCosine(edge);
		feature.simOutcite = srwFeatureExtractor.getOutciteCosine(edge);
		feature.recency = srwFeatureExtractor.getCumulatedRecency(edge);
		feature.positionSim = srwFeatureExtractor.getAuthorPositionSimilarity(edge);
		feature.instituteSimilarity = srwFeatureExtractor.getInstituteSimilarity(edge);
		feature.jaccard = advFeatureExtractor.getJaccardCoef();
		feature.adamic = advFeatureExtractor.getAdamicAdar();
		return feature;
	}
	
	public void processPairs(String path, String cls) throws IOException, SQLException, ParseException, ClassNotFoundException{
		logger.info("Start processing pairs in "+path);
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int count = 0;
		FileWriter writer = new FileWriter(path+".feature");
		  while ((strLine = br.readLine()) != null){
			  String[] fields = strLine.split("\t");
			  if(fields.length >=2 && fields[0].length() >0 && fields[1].length() >0 ){
				  int year = DEFAULT_YEAR;
				  if(fields.length >=3 && fields[2].length() >0)
					  year = Integer.parseInt(fields[2]);
				  AuthorshipEdge edge = new AuthorshipEdge(fields[0], fields[1]);
				  FeatureEntity feature = getFeatureForPair(edge, year);
				  writer.append(feature.toString()).append(",").append(cls).append("\n");
				  count ++;
				  if(count % 100 == 0)
					  logger.info(count+" pairs have been processed.");
			  }
		  }
		  writer.close();
	}
	/**
	 * process pairs for the individual author, the format is
	 * src,dest,connect
	 * Therefore no class label needed.
	 * @param path
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws SQLException 
	 */
	public void processPairsForAuthor(String path) throws IOException, SQLException, ParseException{
		logger.info("Start processing pairs in "+path);
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int count = 0;
		int year = DEFAULT_YEAR;
		FileWriter writer = new FileWriter(path+".feature");
		  while ((strLine = br.readLine()) != null){
			  String[] fields = strLine.split(",");
			  if(fields.length >=3 && fields[0].length() >0 && fields[1].length() >0 && fields[2].length() >0){
				  AuthorshipEdge edge = new AuthorshipEdge(fields[0], fields[1]);
				  FeatureEntity feature = getFeatureForPair(edge, year);
				  writer.append(feature.toString()).append(",").append(fields[2]).append("\n");
				  count ++;
				  if(count % 100 == 0)
					  logger.info(count+" pairs have been processed.");
			  }
		  }
		  writer.close();
		  logger.info(path+" has been processed.");
	}
	
	public void testSinglePair(String src, String dest, int year) throws SQLException, ParseException, IOException, ClassNotFoundException{
		AuthorshipEdge edge = new AuthorshipEdge(src, dest);
		FeatureEntity feature = getFeatureForPair(edge, year);
		System.out.println(getFriendlyOutput(feature));
		System.out.println("-----");
		srwFeatureExtractor.closeAllStuff();
	}
	
	public String getFriendlyOutput(FeatureEntity feature){
		StringBuffer sb = new StringBuffer();
		String colSep = "\t";
		String fieldSep = "\n";
		sb.append("src:").append(colSep).append(feature.src).append(fieldSep);
		sb.append("dest:").append(colSep).append(feature.dest).append(fieldSep);
		sb.append("simfulltext:").append(colSep).append(feature.simFulltex).append(fieldSep);
		sb.append("simMesh:").append(colSep).append(feature.simMesh).append(fieldSep);
		sb.append("simIncite:").append(colSep).append(feature.simIncite).append(fieldSep);
		sb.append("simOutcite:").append(colSep).append(feature.simOutcite).append(fieldSep);
		sb.append("sumPub:").append(colSep).append(feature.sumPub).append(fieldSep);
		sb.append("sumCoauthor:").append(colSep).append(feature.sumCoAuthor).append(fieldSep);
		sb.append("sumCommonFriend:").append(colSep).append(feature.numCommonFriend).append(fieldSep);
		sb.append("sumClusteringCoef:").append(colSep).append(feature.sumClusteringCoef).append(fieldSep);
		sb.append("recency:").append(colSep).append(feature.recency).append(fieldSep);
		sb.append("positionDiff:").append(colSep).append(feature.positionSim).append(fieldSep);
		sb.append("simInstitute:").append(colSep).append(feature.instituteSimilarity).append(fieldSep);
		sb.append("jaccard:").append(colSep).append(feature.jaccard).append(fieldSep);
		sb.append("adamic:").append(colSep).append(feature.adamic).append(fieldSep);
		return sb.toString();
	}
	
	public void closeAllStuff() throws SQLException, IOException{
		if(srwFeatureExtractor != null){
			srwFeatureExtractor.closeAllStuff();
		}
	}
	
	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws SQLException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws NumberFormatException, SQLException, ParseException, IOException, ClassNotFoundException {
		if(args.length != 3){
			System.out.println("SimpleFeatureExtraction, Test single pairs\n--src --dest --year");
			return;
		}
		SimpleFeatureExtractor.DEFAULT_YEAR = Integer.parseInt(args[2]);
		SimpleFeatureExtractor extractor = new SimpleFeatureExtractor();
		extractor.testSinglePair(args[0], args[1], Integer.parseInt(args[2]));
	}

}
