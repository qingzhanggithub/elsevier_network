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
public class AuthorStats {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length !=2){
			System.out.println("--save --start");
			return;
		}
		try {
			MedlineAuthorAnalysis ma = new MedlineAuthorAnalysis();
			ma.getInicteYearForAuthors(args[0], Integer.parseInt(args[1]));
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
