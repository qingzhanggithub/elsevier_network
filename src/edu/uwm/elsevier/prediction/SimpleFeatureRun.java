/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.lucene.queryParser.ParseException;

/**
 * @author qing
 *
 */
public class SimpleFeatureRun {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws ParseException 
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws NumberFormatException, IOException, SQLException, ParseException, ClassNotFoundException {
		if(args.length != 3){
			System.out.println("--path --class --year");
			return;
		}
		SimpleFeatureExtractor.DEFAULT_YEAR = Integer.parseInt(args[2]);
		SimpleFeatureExtractor extractor = new SimpleFeatureExtractor();
		extractor.processPairs(args[0], args[1]);
	}

}
