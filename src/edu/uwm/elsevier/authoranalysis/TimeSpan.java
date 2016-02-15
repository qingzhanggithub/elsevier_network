/**
 * 
 */
package edu.uwm.elsevier.authoranalysis;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author qing
 *
 */
public class TimeSpan {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length != 3){
			System.out.println("--path --save --span-from");
			return;
		}
		try {
			MedlineAuthorAnalysis ma = new MedlineAuthorAnalysis();
//			ma.analyzeCoAuthorshipTimeSpanNormalized(args[0], args[1], Integer.parseInt(args[2]));
			ma.analyzeCoAuthorshipTimeSpan(args[0], args[1]);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
