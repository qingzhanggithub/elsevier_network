/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author qing
 *
 */
public class Labeller {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length != 5){
			System.out.println("Labeller\n--path --training-end-year --test-end-year --pos-size --neg-size");
			return;
		}
		try {
			CoAuthorshipExtractor extractor = new CoAuthorshipExtractor();
			extractor.assignPosNeg(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
