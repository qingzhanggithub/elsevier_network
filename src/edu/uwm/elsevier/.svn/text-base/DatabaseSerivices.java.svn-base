/**
 * 
 */
package edu.uwm.elsevier;

import java.sql.SQLException;

/**
 * @author qing
 *
 */
public class DatabaseSerivices {
	private static CitationNetworkService citationNetworkService =null;
	
	public static CitationNetworkService getCitationNetworkService() throws ClassNotFoundException, SQLException{
		if(citationNetworkService ==null)
			citationNetworkService = new CitationNetworkService();
		return citationNetworkService;
	}
}
