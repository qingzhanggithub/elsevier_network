/**
 * 
 */
package edu.uwm.elsevier.authoranalysis.test;

import java.sql.SQLException;

import edu.uwm.elsevier.authoranalysis.CoAuthorNetwork;

/**
 * @author qing
 *
 */
public class TestCoauthorNetwork {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		if(args.length != 1){
			System.out.println("Test coauthor network\n--pmid");
			return;
		}
		CoAuthorNetwork network = new CoAuthorNetwork(null);
		network.testBuildCoAuthorNetwork(args[0]);
	}

}
