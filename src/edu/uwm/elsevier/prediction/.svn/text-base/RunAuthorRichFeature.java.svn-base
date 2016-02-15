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
public class RunAuthorRichFeature {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException {
		if(args.length != 2){
			System.out.println("--path --year");
			return;
		}
		SimpleFeatureExtractor.DEFAULT_YEAR = Integer.parseInt(args[1]);
		SimpleFeatureExtractor extractor = new SimpleFeatureExtractor();
		extractor.processPairsForAuthor(args[0]);
	}

}
