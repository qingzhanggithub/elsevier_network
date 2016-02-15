/**
 * 
 */
package edu.uwm.elsevier.linkanalysis.matrix;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.namedisambiguation.AuthorityEntity;
import edu.uwm.elsevier.namedisambiguation.AuthorityTool;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.article.Organization;

/**
 * @author qing
 *
 */
public class ResultDisplay {
	
	private ArticleService articleService;
	private AuthorService authorService;
	private AuthorityTool authorityTool;
	private AuthorDSBService authorDSBService;
	private CitationNetworkService citationNetworkService;
	public static String HTML_SPACE="&nbsp;&nbsp;";
	public static String HTML_NEWLINE = "<br>";
	
	public ResultDisplay() throws ClassNotFoundException, SQLException{
		articleService = new ArticleService();
		authorService = new AuthorService();
		authorityTool = new AuthorityTool();
		authorDSBService = new AuthorDSBService();
		citationNetworkService = new CitationNetworkService();
	}
	
	public String readArticleFromDB(int articleId) throws SQLException{
		Article article = articleService.getArticleByArticleId(articleId);
		List<Author> authors = authorService.getAuthorListByArticleId(articleId);
		String articleStr = getArticleString(article);
		String authorStr = getAuthorString(authors);
		return  articleStr+"\n[AUTHORS]\n"+authorStr;
	}
	
	public String readArticleFromDBToHTML(int articleId) throws SQLException{
		Article article = articleService.getArticleByArticleId(articleId);
		List<Author> authors = authorService.getAuthorListByArticleId(articleId);
		String articleHTML = getArticleHTML(article);
		String authorsHTML = getAuthorsHTML(authors);
		StringBuffer sb =new StringBuffer();
		sb.append(articleHTML).append(HTML_NEWLINE);
		sb.append(authorsHTML).append(HTML_NEWLINE);
		return sb.toString();
	}
	
	public String readArticleFromDBToHTMLSimple(int articleId) throws SQLException{
		Article article = articleService.getArticleByArticleId(articleId);
		List<Author> authors = authorService.getAuthorListByArticleId(articleId);
		System.out.println("Number of authors: "+authors.size());
		StringBuffer sb = new StringBuffer();
		for(Author author: authors){
			sb.append(author.getLastName()).append(HTML_SPACE).append(author.getFirstName()).append(",").append(HTML_SPACE);
		}
		sb.append("<b>").append(article.getTitle()).append("</b>.").append(HTML_SPACE).append("<i>").append(article.getJournal()).append("</i>");
		if(article.getDatePublished()!= null)
			sb.append(HTML_SPACE).append(article.getDatePublished());
		System.out.println(sb.toString());
		return sb.toString();
	}
	
	public String readAuthorFromAuthority(String authorityId) throws SQLException{
		AuthorityEntity author = authorityTool.getAuthorByAuthorityId(authorityId);
		List<String> lastNames = author.getLastNameVariations();
		List<String> firstNames = author.getFirstNameVariations();
		String lastName = "NONE";
		if(lastNames.size()>0)
			lastName = lastNames.get(0);
		StringBuffer sb = new StringBuffer();
		sb.append("[").append(author.getAuthorityId()).append("] ").append(lastName);
		if(firstNames.size() >0)
			sb.append(",").append(firstNames.get(0));
		sb.append('\n');
		List<Integer> authorIds = authorDSBService.getAuthorIdsByAuthorityID(authorityId);
		Set<String> orgSet = new HashSet<String>();
		for(int authorId: authorIds){
			List<Organization> orgs = authorService.getOrganizationsByAuthorId(authorId);
			String orgStr = getOrgString(orgs);
			orgSet.add(orgStr);
		}
		for(String org: orgSet){
			sb.append(org).append('\n');
		}
		return sb.toString();
	}
	
	public String readAuthorFromAuthorityToHTML(String authorityId) throws SQLException{
		AuthorityEntity author = authorityTool.getAuthorByAuthorityId(authorityId);
		List<String> lastNames = author.getLastNameVariations();
		List<String> firstNames = author.getFirstNameVariations();
		String lastName = "NONE";
		if(lastNames.size()>0)
			lastName = lastNames.get(0);
		StringBuffer sb = new StringBuffer();
		sb.append("<i><b>").append(lastName).append("</b></i>");
		if(firstNames.size() >0)
			sb.append(",<i><b>").append(firstNames.get(0)).append("</b></i>");
		sb.append(HTML_NEWLINE);
		sb.append("Possible affiliations:").append(HTML_NEWLINE);
		List<Integer> authorIds = authorDSBService.getAuthorIdsByAuthorityID(authorityId);
		Set<String> orgSet = new HashSet<String>();
		for(int authorId: authorIds){
			List<Organization> orgs = authorService.getOrganizationsByAuthorId(authorId);
			String orgStr = getOrgHTML(orgs);
			orgSet.add(orgStr);
		}
		for(String org: orgSet){
			sb.append(org).append(HTML_NEWLINE);
		}
		sb.append("<hr>");
		return sb.toString();
	}
	
	public void writeArticleDisplay(String rankedPath, String save) throws IOException, NumberFormatException, SQLException{
		FileWriter writer = new FileWriter(save);
		FileInputStream fstream = new FileInputStream(AACMatrix.AAC_ROOT+rankedPath);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int i=1;
		while ((strLine = br.readLine()) != null){
			String[] records = strLine.split(",");
			String str = readArticleFromDB(Integer.parseInt(records[1]));
			writer.append("======== RANK ").append(String.valueOf(i)).append(" ========").append('\n');
			writer.append(str).append('\n');
			i++;
			if(i==1000)
				break;
		}
		writer.close();
	}
	
	public void writeAuthorDisplay(String rankedPath, String save) throws IOException, SQLException{
		FileWriter writer = new FileWriter(save);
		FileInputStream fstream = new FileInputStream(AACMatrix.AAC_ROOT+rankedPath);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int i=1;
		while ((strLine = br.readLine()) != null){
			String[] records = strLine.split(",");
			String id = records[1].substring(1, records[1].length()-1);
			writer.append("======== RANK ").append(String.valueOf(i)).append(" ========").append('\n');
			String str = readAuthorFromAuthority(id);
			writer.append(str).append('\n');
			writer.append(records[0]).append('\n');
			i++;
			if(i==1000)
				break;
		}
		writer.close();
	}
	
	public static String getArticleString(Article article){
		StringBuffer sb = new StringBuffer();
		sb.append("[TITLE]\t").append(article.getTitle()).append('\n');
		sb.append("[JOURNAL]\t").append(article.getJournal()).append('\n');
		sb.append("[ARTICLE ID]\t").append(article.getArticleId()).append('\n');
		sb.append("[DATE]\t").append(article.getDatePublished());
		return sb.toString();
	}
	
	public  String getArticleHTML(Article article) throws SQLException{
		StringBuffer sb = new StringBuffer();
		sb.append("<br>").append(article.getTitle());
		sb.append("<br><i>").append(article.getJournal()).append("</i>");
//		sb.append("<br>ID:").append(article.getArticleId());
		sb.append("<br>PMID: ").append(citationNetworkService.getPMIDByArticleId(article.getArticleId()));
		sb.append("<br>").append(article.getDatePublished());
		return sb.toString();
	}
	
	public String getOrgString(List<Organization> orgs){
		StringBuffer sb = new StringBuffer();
		for(Organization org: orgs){
			sb.append(org.getName()).append('\n');
		}
		return sb.toString();
	}
	
	public String getOrgHTML(List<Organization> orgs){
		StringBuffer sb = new StringBuffer();
		for(Organization org: orgs){
//			sb.append("[").append(org.getOrganizationId()).append("]").append(org.getName()).append(HTML_NEWLINE);
			sb.append(org.getName());
		}
		return sb.toString();
	}
	
	public  String getAuthorString(List<Author> authors) throws SQLException{
		StringBuffer sb = new StringBuffer();
		for(Author author: authors){
			List<Organization> org = authorService.getOrganizationsByAuthorId((int)author.getAuthorId());
			String orgStr = getOrgString(org);
			sb.append("[").append(author.getAuthorId()).append("]").append(author.getLastName()).append('\n');
			sb.append(orgStr);
		}
		return sb.toString();
	}
	
	public String getAuthorsHTML(List<Author> authors) throws SQLException{
		StringBuffer sb = new StringBuffer();
		for(Author author: authors){
			sb.append("<b><i>").append(author.getLastName()).append(",").append(author.getFirstName()).append("</i></b>");
			List<Organization> org = authorService.getOrganizationsByAuthorId((int)author.getAuthorId());
			String orgStr = getOrgString(org);
			sb.append(HTML_SPACE).append(orgStr).append(HTML_NEWLINE);
		}
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
		if(args.length !=4){
			System.out.println("--root --type --id --save");
			return;
		}
		ResultDisplay display = new ResultDisplay();
		AACMatrix.AAC_ROOT = args[0];
//		String str ="NONE";
		if(args[1].equals("article"))
			display.writeArticleDisplay(args[2], args[3]);
		else
			display.writeAuthorDisplay(args[2], args[3]);
		System.out.println("Task done.");
	}

}
