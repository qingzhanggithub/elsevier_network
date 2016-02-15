/**
 * 
 */
package edu.uwm.elsevier.prediction.release;

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

import edu.uwm.elsevier.prediction.AuthorshipEdge;
import edu.uwm.elsevier.prediction.FeatureEntity;
import edu.uwm.elsevier.prediction.RecurringFeatureExtractor;

/**
 * @author qing
 *
 */
public class SimpleFeature {
	
	private Logger logger = Logger.getLogger(SimpleFeature.class);
	private RecurringFeatureExtractor extractor ;
	public SimpleFeature() throws ClassNotFoundException, SQLException, IOException{
		extractor = new RecurringFeatureExtractor();
	}
	
	
	public void extractFeatures(String path, String save, String clsLabel, int startYear, int endYear) throws IOException, SQLException, ParseException{
		logger.info("Start processing pairs in "+path);
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int count = 0;
		FileWriter writer = new FileWriter(save);
		  while ((strLine = br.readLine()) != null){
			  String[] fields = strLine.split(",");
			  if(fields.length >=2 && fields[0].length() >0 && fields[1].length() >0 ){
				  AuthorshipEdge edge = new AuthorshipEdge(fields[0], fields[1]);
				  FeatureEntity feature = extractor.getSimpleFeature(edge, startYear, endYear);
				  if(feature != null){
					  writer.append(getString(feature)).append(",").append(clsLabel).append("\n");
					  count ++;
					  if(count % 50 == 0)
						  logger.info(count+" pairs have been processed.");
				  }
			  }
		  }
		  writer.close();
		  br.close();
		  logger.info("Done.");
	}
	
	public String getString(FeatureEntity feature){
		StringBuffer sb = new StringBuffer();
		sb.append(feature.getSrc()).append(',');
		sb.append(feature.getDest()).append(',');
		sb.append(feature.getSimFulltex()).append(',');
		sb.append(feature.getSimMesh()).append(',');
		sb.append(feature.getSumPub()).append(',');
		sb.append(feature.getSumCoAuthor()).append(',');
		sb.append(feature.getNumCommonFriend()).append(',');
		sb.append(feature.getSumClusteringCoef());
		return sb.toString();
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws ParseException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, NumberFormatException, ParseException {
		if(args.length != 5){
			System.out.println("--path --save --class --start-year --end-year");
			return;
		}
		SimpleFeature simpleFeature = new SimpleFeature();
		simpleFeature.extractFeatures(args[0], args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));

	}

}
