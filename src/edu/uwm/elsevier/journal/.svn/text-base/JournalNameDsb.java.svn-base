/**
 * 
 */
package edu.uwm.elsevier.journal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import articlesdata.article.Article;
import articlesdata.database.ArticlesDataDBConnection;
import edu.uwm.elsevier.CitationArticleComparison;
import edu.uwm.elsevier.ITableNames;

/**
 * @author qing
 *
 */
public class JournalNameDsb {
	
	private ArticlesDataDBConnection databaseConnection;
	public static String JOURNAL_TOKENIZATION_REGEX = "[\\s\\?!,.:<>\\(\\)&]";

	public JournalNameDsb() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	
	public JournalRecord groupJournalNamesByInitialsFromDB(Article article){
		if(article.getJournal() !=null){
			String key = getInitialKey(article.getJournal());
			JournalRecord record = new JournalRecord(article.getArticleId(), article.getJournal(), key);
			return record;
		}
		return null;
	}
	
	public String getInitialKey(String journal){
		List<String> tokens = CitationArticleComparison.tokenizationAndRemoveStopWords(journal, JOURNAL_TOKENIZATION_REGEX);
		StringBuffer sb = new StringBuffer();
		for(String token: tokens){
			sb.append(token.charAt(0));
		}
		return sb.toString();
	}
	
	public void closeAllStuff() throws SQLException{
		if(databaseConnection !=null){
			databaseConnection.close();
		}
	}
	
	public void dsbJournalNames() throws SQLException{
		String sql = "select distinct(initial_key) from elsevier_journal";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<String> initKeys = new ArrayList<String>();
		while(rs.next()){
			initKeys.add(rs.getString(1));
		}
		rs.close();
		for(String initKey: initKeys){
			List<DisambiguatedJournalRecord> journals = getJournalNames(initKey);
		}
	}
	
	public List<DisambiguatedJournalRecord> getJournalNames(String initKey) throws SQLException{
		List<DisambiguatedJournalRecord> journals = new ArrayList<DisambiguatedJournalRecord>();
		String sql = "select distinct(journal) from elsevier_journal where initial_key=\'"+initKey+"\'";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			DisambiguatedJournalRecord record = new DisambiguatedJournalRecord();
			record.setInitialKey(initKey);
			record.setJournal(rs.getString(1));
			journals.add(record);
		}
		return journals ;
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
