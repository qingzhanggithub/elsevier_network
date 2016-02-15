/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import articlesdata.database.ArticlesDataDBConnection;

import pmidmapper.MedlineSearcher;
import pmidmapper.PMArticle;

import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.namedisambiguation.AuthorityTool;

/**
 * @author qing
 *
 */
public class GoogleScholar {
	
	private ArticlesDataDBConnection connection;
	private MedlineSearcher searcher;
	private Logger logger = Logger.getLogger(GoogleScholar.class);
	
	public GoogleScholar() throws ClassNotFoundException, SQLException, IOException{
		connection = ArticlesDataDBConnection.getInstance();
		searcher = new MedlineSearcher(MedlineSearcher.defaultMedlineIndexPath);
	}
	
	public void getLatestPublication(String path) throws IOException, NumberFormatException, SQLException, ParseException{
		logger.info("Start processing "+path);
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int count = 0;
		FileWriter pmidWriter = new FileWriter(path+".latestpub");
		FileWriter titleWriter = new FileWriter(path+".title");
		while ((strLine = br.readLine()) != null){
			String[] fields = strLine.split("\t");
			long pmid = getPMID(fields[0], fields[1],Integer.parseInt(fields[4]));
			if(pmid == -1)
				continue;
			PMArticle article = searcher.getPMArticleByPMID(pmid, false);
			String title = article.getTitle();
			pmidWriter.append(strLine).append('\t').append(String.valueOf(pmid)).append('\n');
			titleWriter.append(strLine).append('\t').append(String.valueOf(pmid)).append('\n');
			titleWriter.append(title).append('\n');
			count++;
			if(count % 100 == 0){
				logger.info(count+" has been processed.");
			}
		}
		pmidWriter.close();
		titleWriter.close();
		br.close();
		in.close();
		fstream.close();
		logger.info("Task done. "+path+" has been processed.");
	}
	
	public long getPMID(String src, String dest, int year) throws SQLException{
		String sql ="select pmid, year from "+ITableNames.CO_AUTHOR
				+" where authority_author_id=\'"+AuthorityTool.escape(src)+"\'" +
						" and co_author_authority_author_id=\'"+AuthorityTool.escape(dest)+"\' and year="+year+" order by year desc";
		Statement stmt = connection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		long pmid = -1;
		if(rs.next()){
			pmid = rs.getLong(1);
		}
		return pmid;
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws ParseException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, NumberFormatException, ParseException {
		if(args.length != 1){
			System.out.println("--path");
			return;
		}
		
		GoogleScholar scholar = new GoogleScholar();
		scholar.getLatestPublication(args[0]);

	}

}
