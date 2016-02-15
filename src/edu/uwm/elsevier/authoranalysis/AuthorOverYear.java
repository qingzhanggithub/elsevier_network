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
public class AuthorOverYear {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			MedlineAuthorAnalysis ma = new MedlineAuthorAnalysis();
			ma.analyzeAuthorOverYearNormalized(args[0]);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
