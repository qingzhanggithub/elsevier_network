/**
 * 
 */
package edu.uwm.elsevier.misc;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.CitationNetworkService;

/**
 * @author qing
 *
 */
public class PubMedSentence {
	
	private CitationNetworkService citationNetworkService;
	private ArticlesDataDBConnection connection;
	private String root = "/home/qzhang/pubmed_sentences/";
	
	public PubMedSentence() throws ClassNotFoundException, SQLException{
		citationNetworkService = new CitationNetworkService();
		connection = ArticlesDataDBConnection.getInstance();
	}
	
	public  void getArticleByPmid(long pmid) throws SQLException, IOException{
		List<Integer> ids = citationNetworkService.getArticleIdByPMID(pmid);
		FileWriter writer = new FileWriter(root+pmid+".txt");
		if(ids != null && ids.size() == 1){
			int articleId = ids.get(0);
			writer.append(getSentencesByArticleId(articleId));
		}
		writer.close();
	}
	
	public String getSentencesByArticleId(int articleId) throws SQLException{
		String sql = "select text from sentence where article_id ="+articleId+" order by sentence_id";
		StringBuffer sb = new StringBuffer();
		Statement stmt = connection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			sb.append(rs.getString(1)).append("\n");
		}
		rs.close();
		stmt.close();
		return sb.toString();
	}

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NumberFormatException, IOException {
		if(args.length != 1){
			System.out.println("--pmid");
			return;
		}
		
		PubMedSentence pms = new PubMedSentence();
		pms.getArticleByPmid(Long.parseLong(args[0]));

	}

}
