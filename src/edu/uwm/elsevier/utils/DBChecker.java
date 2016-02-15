/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.uwm.elsevier.ElsevierArticleMetaDataSearcher;
import edu.uwm.elsevier.NetworkBuilderLogger;

import articlesdata.citation.CitationParserUtils;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class DBChecker {
	
	private static Logger LOGGER =  NetworkBuilderLogger.getLogger("DBchecker");
	
	public static void check(int begin) throws ClassNotFoundException, SQLException {
		ArticlesDataDBConnection dbconn = ArticlesDataDBConnection.getInstance();
		String sql = "select citation_qing.citation_id, citation_text from citation_qing, citation_detail" ;
		
		String bound = "citation_qing.citation_id = citation_detail.citation_id and is_originally_parsed = 0";
		Statement stmt = dbconn.getConnection().createStatement();
		int start = begin;
		int page = 100000;
		ResultSet rs = null;
		while(start < 90000000){
			LOGGER.info("start="+start);
			String q = sql + " where citation_qing.citation_id >"+ start +" and citation_qing.citation_id < "+(start+page)+" and "+bound;
			rs = stmt.executeQuery(q);
			while(rs.next()){
				String text = rs.getString("citation_text");
				long citationId = rs.getLong("citation_id");
				if(text !=null){
					ArrayList<String> tokens 
					= CitationParserUtils.simpleTokenization(text, ElsevierArticleMetaDataSearcher.TEXT_TOKENIZE_PATTERN);
					if(tokens.size() > 1024){
						LOGGER.error(citationId+"\n"+text+"\nsize="+tokens.size());
						return;
					}
				}
			}
			start+= page;
		}
	}
	
	public static void main(String[] args){
		if(args.length !=1){
			System.out.println("--begin");
			return;
		}
		try {
			check(Integer.parseInt(args[0]));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
