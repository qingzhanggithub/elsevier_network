/**
 * 
 */
package edu.uwm.elsevier.authoranalysis;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.log4j.Logger;
import com.csvreader.CsvReader;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;

/**
 * @author qing
 *
 */
public class CoAuthorNetwork {
	
	private AuthorDSBService authorDSBService;
	private CitationNetworkService citationNetworkService;
	private String pmidPath = "/home/qzhang/network_analysis/authority_pmid.csv";
	private Logger logger = Logger.getLogger(CoAuthorNetwork.class);
	
	public CoAuthorNetwork(String pmidPath) throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
		citationNetworkService = new CitationNetworkService();
		this.pmidPath = pmidPath;
	}
	/**
	 * build coauthor network. BE NOTICED : there are single author works so this network cannot be used to retrieve publications.
	 * @param save
	 * @throws IOException
	 * @throws NumberFormatException
	 * @throws SQLException
	 */
	public void buildCoAuthorNetwork(String save) throws IOException, NumberFormatException, SQLException{
		logger.info("Task Start. Building co-author network ...");
		CsvReader csv = new CsvReader(pmidPath);
		FileWriter writer = new FileWriter(save);
		int count =0;
		while(csv.readRecord()){
			count++;
			String pmid = csv.get(0);
			int year = citationNetworkService.getYearByPMID(Long.parseLong(pmid));
			List<String> authors = authorDSBService.getAuthorsByPmid(Long.parseLong(pmid));
			int size = authors.size();
			for(int i=0;i<size; i++){
				String src = authors.get(i);
				for(int j=i+1; j< size; j++){
					String dest = authors.get(j);
					if(src.equals(dest))
						logger.error("src=dest:"+src+"\tpmid="+pmid);
					else{
						writer.append(src).append('\t').append(dest).append('\t').append(pmid).append('\t').append(String.valueOf(year)).append('\n');
						writer.append(dest).append('\t').append(src).append('\t').append(pmid).append('\t').append(String.valueOf(year)).append('\n');
					}
				}
			}
			if(count %1000 == 0){
				logger.info(count+" pmids have been processed.");
			}
		}
		writer.close();
		csv.close();
	}
	
	
	public void testBuildCoAuthorNetwork(String pmid) throws NumberFormatException, SQLException{
		int year = citationNetworkService.getYearByPMID(Long.parseLong(pmid));
		List<String> authors = authorDSBService.getAuthorsByPmid(Long.parseLong(pmid));
		int size = authors.size();
		System.out.println("Number of authors: "+size);
		StringBuffer sb= new StringBuffer();
		for(int i=0;i<size; i++){
			String src = authors.get(i);
			for(int j=i+1; j< size; j++){
				String dest = authors.get(j);
				if(src.equals(dest))
					logger.error("src=dest:"+src+"\tpmid="+pmid);
				else{
					sb.append(src).append('\t').append(dest).append('\t').append(pmid).append('\t').append(String.valueOf(year)).append('\n');
					sb.append(dest).append('\t').append(src).append('\t').append(pmid).append('\t').append(String.valueOf(year)).append('\n');
				}
			}
		}
		System.out.println(sb.toString());
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length !=2){
			System.out.println("--pmid-path --save");
			return;
		}
		try {
			CoAuthorNetwork conetwork = new CoAuthorNetwork(args[0]);
			conetwork.buildCoAuthorNetwork(args[1]);
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
