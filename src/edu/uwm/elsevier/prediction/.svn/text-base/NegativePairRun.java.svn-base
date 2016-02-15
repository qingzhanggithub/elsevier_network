/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author qing
 *
 */
public class NegativePairRun {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws NumberFormatException, ClassNotFoundException, SQLException, IOException {
		if(args.length != 4){
			System.out.println("Negative pair sampler\n--year-start --year-end --size --save");
			return;
		}
		CoAuthorshipExtractor extractor = new CoAuthorshipExtractor();
		List<AuthorshipEdge> pairs = extractor.getNegativeAuthorPairs(Integer.parseInt(args[0]),Integer.parseInt(args[1]), Integer.parseInt(args[2]));
		FileWriter writer = new FileWriter(args[3]+"_"+args[0]+"_"+args[1]+args[2]+".csv");
		for(AuthorshipEdge edge: pairs){
			writer.append(edge.src).append("\t").append(edge.dest).append("\n");
		}
		writer.close();
	}

}
