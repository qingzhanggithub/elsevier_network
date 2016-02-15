/**
 * 
 */
package edu.uwm.elsevier.authoranalysis;

import java.sql.SQLException;

/**
 * @author qing
 *
 */
public class ConnectedComponentEntry {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length != 1){
			System.out.println("--begin");
			return;
		}
		try {
			MedlineAuthorAnalysis ma = new MedlineAuthorAnalysis();
			ma.analyzeConnectedComponents(Integer.parseInt(args[0]));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

}
