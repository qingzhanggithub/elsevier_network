package edu.uwm.elsevier.utils;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.csvreader.CsvReader;

import edu.uwm.elsevier.ITableNames;

import articlesdata.database.ArticlesDataDBConnection;

public class GeneOntology {

	private ArticlesDataDBConnection articlesDataDBConnection;
	private Logger logger = Logger.getLogger(GeneOntology.class);
	
	public GeneOntology() throws ClassNotFoundException, SQLException{
		articlesDataDBConnection = ArticlesDataDBConnection.getInstance();
	}
	
	public void countGOinDB(String path, String save) throws IOException, SQLException{
		CsvReader csv = new CsvReader(path);
		FileWriter writer = new FileWriter(save);
		int count = 0;
		int total = 0;
		while(csv.readRecord()){
			total++;
			String pmid = csv.get(0);
			if(isInDB(pmid)){
				count++;
				writer.append(pmid).append('\n');
			}
				
			if(total%100==0)
				logger.info(total);
				
		}
		writer.close();
		System.out.println("total="+total+", count="+count);
	}
	
	public boolean isInDB(String pmid) throws SQLException{
		String sql = "select * from "+ITableNames.MEDLINE_NETWORK_STAT_TABLE+" where pmid="+pmid;
		Statement stmt = articlesDataDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		boolean has = rs.next();
		rs.close();
		stmt.close();
		return has;
		
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
//		if(args.length !=2){
//			System.out.println("--path --save");
//			return;
//		}
		String path = "/Users/qing/dev/go_pmids.csv";
		String save="/Users/qing/dev/go_db_found.csv";
		GeneOntology go = new GeneOntology();
		go.countGOinDB(path,save);
	}

}
