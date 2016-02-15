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
public class AuthorYearSpan {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length != 1){
			System.out.println("--save");
			return;
		}
		try {
			MedlineAuthorAnalysis ma = new MedlineAuthorAnalysis();
			ma.getAuthorYearSpan(args[0]);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
