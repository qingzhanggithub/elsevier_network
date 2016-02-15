/**
 * 
 */
package edu.uwm.elsevier;

/**
 * @author qing
 *
 */
public class Configuration {
	private static Configuration singleton = null;
	private static String pubmedIndex = "/Users/qing/datauser/data_user/pubmed_index";
	private static String articleIndex = "/Users/qing/wind3/elsevier_index";
	private static String citationIndex = "/Users/qing/wind3/elsevier_citation_index2";
	private static String database = null;
	
	public static void getConfigurationInstance(String machine){
		
		if(singleton == null){
			if(machine.equalsIgnoreCase("local")){
				pubmedIndex = "/Users/qing/datauser/data_user/pubmed_index";
				articleIndex = "/Users/qing/wind3/elsevier_index";
				citationIndex = "/Users/qing/wind3/elsevier_citation_index2"; 
				database = "jdbc:mysql://snake.ims.uwm.edu:3306/articles_data_2";
			}else if(machine.equalsIgnoreCase("server")){
				String server = "/home/qzhang";
				pubmedIndex = "/home/data_user/pubmed_index";
				articleIndex = server+"/elsevier_index";
				citationIndex = server+"/elsevier_citation_index2";
				database = "jdbc:mysql://compute-0-10:3306/articles_data_2";
			}
		}
	}

}
