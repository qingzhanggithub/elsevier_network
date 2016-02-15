/**
 * 
 */
package edu.uwm.elsevier.evaluation;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.lucene.queryParser.ParseException;

import pmidmapper.MedlineSearcher;
import pmidmapper.PMArticle;

import edu.uwm.elsevier.ElsevierArticle;
import edu.uwm.elsevier.MappingStatus;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.AuthorService;
import articlesdata.article.Citation;
import articlesdata.citation.CitationService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class Sampler {

	private String select_cnetwork = "select * from cnetworkv4 where is_matched = 1 limit ?, 1";
	private String select_pmid_mapping  = "select * from elsevier_pmid_mapping where is_matched = 1 limit ?,1";
	private String select_citation_merging = "select * from cnetworkv4_citation_merger where is_matched =1 limit ?, 1";
	private Connection conn;
	private ArticleService articleService;
	private CitationService citationService;
	private AuthorService authorService;
	public static String HTML_SPACE = "&nbsp;&nbsp;";
	public static String HTML_NEWLINE = "<br>";
	public Sampler() throws ClassNotFoundException, SQLException{
		conn = ArticlesDataDBConnection.getInstance().getConnection();
		articleService = new ArticleService();
		citationService = new CitationService();
		authorService = new AuthorService();
	}
	
	public static HashSet<Integer> getSeeds(int size, int max){
		HashSet<Integer> seeds = new HashSet<Integer>();
		Random rand = new Random();
		while(seeds.size() < size){
			seeds.add(rand.nextInt(max)+1);
		}
		return seeds;
 	}
	
	public void sampleCitationMapping(int size, String save) throws SQLException, IOException{
//		String maxSql = "select count(*) from cnetworkv4";
//		Statement stmt = conn.createStatement();
//		ResultSet rs = stmt.executeQuery(maxSql);
//		rs.next();
//		int max = rs.getInt(1);
//		rs.close();
		ResultSet rs;
		PreparedStatement prepSelect = conn.prepareStatement(select_cnetwork);
		HashSet<Integer> seeds = getSeeds(size, 90000);
		List<MappingStatus> statusList = new ArrayList<MappingStatus>();
		for(Integer seed: seeds){
			System.out.print("seed\t"+seed);
			prepSelect.setInt(1, seed);
			rs = prepSelect.executeQuery();
			rs.next();
			MappingStatus status = mapRsToMappingStatus(rs);
			statusList.add(status);
			System.out.println("\tcitation_id="+status.getCitationId());
			rs.close();
		}
		FileWriter writer = new FileWriter(save);
		writer.append(getCitationMappingReport(statusList));
		writer.close();
	}
	
	
	public void sampleCitationMerging(int size, String save) throws SQLException, IOException{
//		String maxSql = "select count(*) from cnetworkv4_citation_merging";
//		Statement stmt = conn.createStatement();
//		ResultSet rs = stmt.executeQuery(maxSql);
//		rs.next();
//		int max = rs.getInt(1);
//		rs.close();
		ResultSet rs;
		HashSet<Integer> seeds = getSeeds(size, 90000);
		PreparedStatement prepSelect = conn.prepareStatement(select_citation_merging);
		List<MappingStatus> statusList = new ArrayList<MappingStatus>();
		for(Integer seed: seeds){
			System.out.println(seed);
			prepSelect.setInt(1, seed);
			rs = prepSelect.executeQuery();
			rs.next();
			MappingStatus status = mapRsToCitationMergingStatus(rs);
			statusList.add(status);
			rs.close();
		}
		FileWriter writer = new FileWriter(save);
		writer.append(getCitationMergingReport(statusList));
		writer.close();
	}
	
	public void sampleElsevierPMIDMapping(int size, String save) throws SQLException, IOException, ParseException{
		ResultSet rs;
		HashSet<Integer> seeds = getSeeds(size, 90000);
		PreparedStatement prepSelect = conn.prepareStatement(select_pmid_mapping);
		List<MappingStatus> statusList = new ArrayList<MappingStatus>();
		for(Integer seed: seeds){
			System.out.println(seed);
			prepSelect.setInt(1, seed);
			rs = prepSelect.executeQuery();
			rs.next();
			MappingStatus status = mapRsToElsevierPMIDMappingStatus(rs);
			statusList.add(status);
			rs.close();
		}
		FileWriter writer = new FileWriter(save);
		writer.append(getElsevierPMIDMappingReport(statusList));
		writer.close();
	}
	
	public String getElsevierPMIDMappingReport(List<MappingStatus> statusList) throws SQLException, IOException, ParseException{
		int size = statusList.size();
		StringBuffer sb = new StringBuffer();
		sb.append("<h3>Elsevier-MEDLINE Mapping Result Sample</h3><br>");
		sb.append("<table border = 1 align = center>");
		sb.append("<tr><td><Num></td><td>Elsevier</td><td>MEDLINE</td></tr>");
		MedlineSearcher searcher = new MedlineSearcher("/Users/qing/datauser/data_user/pubmed_index");
		for( int i=0; i< size; i++){
			Article article = articleService.getArticleByArticleId(statusList.get(i).getArticleId());
			List<articlesdata.article.Author> authorA = authorService.getAuthorListByArticleId(statusList.get(i).getArticleId());
			PMArticle pmArticle = searcher.getPMArticleByPMID(statusList.get(i).getPmid(), false);
			sb.append(getElsevierPMIDMappingReportRowHTML(i, article, authorA, pmArticle));
		}
		return sb.toString();
		
	}
	
	public String getElsevierPMIDMappingReportRowHTML(int index , Article article, List<articlesdata.article.Author> authorA, PMArticle pmArticle){
		StringBuffer sb = new StringBuffer();
		sb.append("<tr>");
		sb.append("<td>").append(index).append("</td>");
		sb.append("<td>");
		sb.append(getArticleHTML(article, authorA));
		sb.append("</td>");
		sb.append("<td>");
		sb.append(getPMArticleHTML(pmArticle));
		sb.append("</td>");
		sb.append("<td>");
		sb.append("<input type=\"checkbox\" name=\"check\" />");
		sb.append("</td>");
		sb.append("</tr>");
		return sb.toString();
	}
	
	public String getCitationMergingReport(List<MappingStatus> statusList) throws SQLException{
		int size = statusList.size();
		StringBuffer sb = new StringBuffer();
		sb.append("<h3>Citation Merging Result Sample (citaiton-citation mapping)</h3><br>");
		sb.append("<table border = 1 align = center>");
		sb.append("<tr><td><Num></td><td>Citation</td><td>Citation</td></tr>");
		for( int i=0; i< size; i++){
			Citation citation = citationService.getCitationByCitationId(statusList.get(i).getCitationId());
			Citation targetCitation = citationService.getCitationByCitationId(statusList.get(i).getTargetCitationId());
			sb.append(getCitationMergingReportRowHTML(i, citation, targetCitation));
		}
		sb.append("</table>");
		return sb.toString();
	}
	
	public String getCitationMappingReport(List<MappingStatus> statusList) throws SQLException{
		int size = statusList.size();
		StringBuffer sb = new StringBuffer();
		sb.append("<h3> Citation Mapping Sample (citation-article mapping)<h3><br>");
		sb.append("<table border = 1 align = center>");
		sb.append("<tr><td><Num></td><td>Citation</td><td>Article</td></tr>");
		for(int i=0; i< size; i++){
			Citation citation = citationService.getCitationByCitationId(statusList.get(i).getCitationId());
			Article article = articleService.getArticleByArticleId(statusList.get(i).getArticleId());
			List<articlesdata.article.Author> authorA = authorService.getAuthorListByArticleId(statusList.get(i).getArticleId());
			sb.append(getCitationMappingReportRowHTML(i, citation, article, authorA));
		}
		sb.append("</table>");
		return sb.toString();
	}
	
	private String getCitationMappingReportRowHTML(int index, Citation citation, Article article, List<articlesdata.article.Author> authorA){
		StringBuffer sb = new StringBuffer();
		sb.append("<tr>");
		sb.append("<td>").append(index).append("</td>");
		sb.append("<td>");
		sb.append(getCitationHTML(citation));
		sb.append("</td>");
		sb.append("<td>");
		sb.append(getArticleHTML(article, authorA));
		sb.append("</td>");
		sb.append("<td>");
		sb.append("<input type=\"checkbox\" name=\"check\" />");
		sb.append("</td>");
		sb.append("</tr>");
		return sb.toString();
	}
	
	private String getCitationMergingReportRowHTML(int index, Citation citation, Citation targetCitation){
		StringBuffer sb = new StringBuffer();
		sb.append("<tr>");
		sb.append("<td>").append(index).append("</td>");
		sb.append("<td>");
		sb.append(getCitationHTML(citation));
		sb.append("</td>");
		sb.append("<td>");
		sb.append(getCitationHTML(targetCitation));
		sb.append("</td>");
		sb.append("<td>");
		sb.append("<input type=\"checkbox\" name=\"check\" />");
		sb.append("</td>");
		sb.append("</tr>");
		return sb.toString();
	}
	
	public String getCitationMappingReport(MappingStatus status ) throws IOException, SQLException{
			StringBuffer sb = new StringBuffer();
			sb.append(status.toString());
			Citation citation = citationService.getCitationByCitationId(status.getCitationId());
			sb.append("CITATION----\n").append(getCitationString(citation));
			if(status.getArticleId() !=0){
				Article article = articleService.getArticleByArticleId(status.getArticleId());
				List<articlesdata.article.Author> authorA = authorService.getAuthorListByArticleId(status.getArticleId());
				sb.append("ARTICLE----\n").append(getArticleString(article, authorA));
			}
			System.out.println(sb.toString());
			return sb.toString();
	}
	
	public String getArticleString(Article article, List<articlesdata.article.Author> authors){
		StringBuffer sb = new StringBuffer();
		sb.append("[article_id]\t").append(article.getArticleId()).append('\n');
		sb.append("[title]\t").append(article.getTitle()).append('\n');
		sb.append("[journal]\t").append(article.getJournal()).append('\n');
		for(articlesdata.article.Author a: authors){
			sb.append(a.getLastName()).append('\t').append(a.getFirstName()).append('\n');
		}
		return sb.toString();
	}
	
	public static String getArticleHTML(Article article, List<articlesdata.article.Author> authors){
		StringBuffer sb = new StringBuffer();
		sb.append("<b>[article_id]</b>").append(HTML_SPACE).append(article.getArticleId()).append(HTML_NEWLINE);
		sb.append("<b>[title]</b>").append(HTML_SPACE).append(article.getTitle()).append(HTML_NEWLINE);
		sb.append("<b>[journal]</b>").append(HTML_SPACE).append(article.getJournal()).append(HTML_NEWLINE);
		sb.append("<b>[authors]</b>").append(HTML_NEWLINE);
		for(articlesdata.article.Author a: authors){
			sb.append(a.getLastName()).append(HTML_SPACE).append(a.getFirstName()).append(HTML_NEWLINE);
			
		}
		sb.append("<b>[year]</b>").append(HTML_SPACE).append(article.getDatePublished());
		return sb.toString();
	}
	
	public static String getElsevierArticleHTML(ElsevierArticle article){
		StringBuffer sb = new StringBuffer();
		sb.append("<b>[article_id]</b>").append(HTML_SPACE).append(article.getArticleId()).append(HTML_NEWLINE);
		sb.append("<b>[title]</b>").append(HTML_SPACE).append(article.getTitle()).append(HTML_NEWLINE);
		sb.append("<b>[journal]</b>").append(HTML_SPACE).append(article.getJournal()).append(HTML_NEWLINE);
		sb.append("<b>[authors]</b>").append(HTML_NEWLINE);
		for(articlesdata.article.Author a: article.getAuthors()){
			sb.append(a.getLastName()).append(HTML_SPACE).append(a.getFirstName()).append(HTML_NEWLINE);
		}
		sb.append("<b>[year]</b>").append(HTML_SPACE).append(article.getDatePublished());
		return sb.toString();
	}
	
	public static String getPMArticleHTML(PMArticle article){
		StringBuffer sb = new StringBuffer();
		sb.append("<b>[pmid]</b>").append(HTML_SPACE).append(article.getPmid()).append(HTML_NEWLINE);
		sb.append("<b>[title]</b>").append(HTML_SPACE).append(article.getTitle()).append(HTML_NEWLINE);
		sb.append("<b>[journal]</b>").append(HTML_SPACE).append(article.getJournal()).append(HTML_NEWLINE);
		sb.append("<b>[authors]</b>").append(HTML_NEWLINE);
		for(articlesdata.article.Author a: article.getAuthorList()){
			sb.append(a.getLastName()).append(HTML_SPACE).append(a.getFirstName()).append(HTML_NEWLINE);
		}
		sb.append("<b>[year]</b>").append(HTML_SPACE).append(article.getYear());
		return sb.toString();
	}
	
	public String getCitationString(Citation citation){
		StringBuffer sb = new StringBuffer();
		sb.append("[citation_id]\t").append(citation.getCitationId()).append('\n');
		sb.append("[title]\t").append(citation.getTitle()).append('\n');
		sb.append("[journal]\t").append(citation.getSource()).append('\n');
		if(citation.getAuthors()!=null){
			for(articlesdata.citation.Author a: citation.getAuthors()){
				sb.append(a.getSurname()).append('\t').append(a.getGivenNameString()).append('\n');
			}
		}
		return sb.toString();
	}
	
	public static  String getCitationHTML(Citation citation){
		StringBuffer sb = new StringBuffer();
		sb.append("<b>[citation_id]</b>").append(HTML_SPACE).append(citation.getCitationId()).append(HTML_NEWLINE);
		sb.append("<b>[title]</b>").append(HTML_SPACE).append(citation.getTitle()).append(HTML_NEWLINE);
		sb.append("<b>[journal]</b>").append(HTML_SPACE).append(citation.getSource()).append(HTML_NEWLINE);
		sb.append("<b>[authors]</b>").append(HTML_NEWLINE);
		if(citation.getAuthors()!=null){
			for(articlesdata.citation.Author a: citation.getAuthors()){
				sb.append(a.getSurname()).append(HTML_SPACE).append(a.getGivenNameString()).append(HTML_NEWLINE);
			}
		}
		sb.append("<b>[Original Text]</b>").append(HTML_SPACE).append(citation.getCitationText());
		
		return sb.toString();
	}
	
	
	public MappingStatus mapRsToMappingStatus(ResultSet rs) throws SQLException{
		MappingStatus status = new MappingStatus(rs.getLong("citation_id"));
		status.setArticleId(rs.getInt("article_id"));
		status.setTitleComp(rs.getInt("title_comp"));
		status.setAuthorsComp(rs.getInt("authors_comp"));
		status.setJournalComp(rs.getInt("journal_comp"));
		status.setIsMatched(rs.getInt("is_matched"));
		return status;
	}
	
	public MappingStatus mapRsToCitationMergingStatus(ResultSet rs) throws SQLException{
		MappingStatus status = new MappingStatus(rs.getLong("citation_id"));
		status.setTargetCitationId(rs.getLong("target_citation_id"));
		status.setTitleComp(rs.getInt("title_comp"));
		status.setAuthorsComp(rs.getInt("authors_comp"));
		status.setJournalComp(rs.getInt("journal_comp"));
		status.setIsMatched(rs.getInt("is_matched"));
		return status;
	}
	
	public MappingStatus mapRsToElsevierPMIDMappingStatus(ResultSet rs) throws SQLException{
		MappingStatus status = new MappingStatus();
		status.setArticleId(rs.getInt("article_id"));
		status.setPmid(rs.getLong("pmid"));
		status.setTitleComp(rs.getInt("title_comp"));
		status.setAuthorsComp(rs.getInt("authors_comp"));
		status.setJournalComp(rs.getInt("journal_comp"));
		status.setIsMatched(rs.getInt("is_matched"));
		return status;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length !=2){
			System.out.println("--size --save");
			return;
		}
		try {
			Sampler sampler = new Sampler();
			sampler.sampleCitationMapping(Integer.parseInt(args[0]), args[1]+"-citation-mapping.html");
			sampler.sampleCitationMerging(Integer.parseInt(args[0]), args[1]+"-citation-merging.html");
			sampler.sampleElsevierPMIDMapping(Integer.parseInt(args[0]), args[1]+"-elsevier-pmid-mapping.html");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}

}
