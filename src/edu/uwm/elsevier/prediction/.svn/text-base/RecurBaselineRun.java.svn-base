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
public class RecurBaselineRun {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws ParseException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, NumberFormatException, ParseException {
		if(args.length != 3){
			System.out.println("--pair-path --class-label --year");
			return;
		}
		
		RecurringFeatureExtractor extractor = new RecurringFeatureExtractor();
		extractor.processBaselinePairs(args[0], args[1], Integer.parseInt(args[2]));
		System.out.println("=== Baseline Task Done. ===");
	}

}
