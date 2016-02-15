/**
 * 
 */
package edu.uwm.elsevier.ranking.evaluation;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.uwm.elsevier.linkanalysis.matrix.ResultDisplay;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.utils.IOUtils;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class HTMLContent {
	
	public static String SAVE_ROOT = "/home/qzhang/subgraphs/evaluation/";
	public static String DATA_ROOT = "/home/qzhang/subgraphs/";
	public static String TOPIC="bc";
	
	private ResultDisplay resultDisplay;
	private ArticleService articleService;
	private AuthorDSBService authorDSBService;
	
	public HTMLContent() throws ClassNotFoundException, SQLException{
		resultDisplay = new ResultDisplay();
		authorDSBService = new AuthorDSBService();
		articleService = new ArticleService();
	}
	
	public void genArticlePage(List<Article> articles, String save) throws IOException, SQLException{
		FileWriter writer = new FileWriter(save);
		for(Article article : articles){
			
			writer.append("<tr>");
			writer.append("<td>");
//			writer.append("<a href=\'").append("lists/").append(TOPIC+"_article_details.html#"+article.getArticleId()+"\'>");
			writer.append(resultDisplay.readArticleFromDBToHTMLSimple(article.getArticleId()));
			writer.append("</td></tr>");
//			writer.append("<br>");
			String name = "au"+article.getArticleId();
			writer.append(getChoiceSnippetSimple(name));
		}
		writer.close();
	}
	
	public void genArticleDetailPage(List<Article> articles, String save) throws IOException, SQLException{
		System.out.println("Generating article detail page ...");
		FileWriter writer = new FileWriter(save);
		for(Article article: articles){
			writer.append("<tr>");
			writer.append("<td><a name=\""+article.getArticleId()+"\">").append(resultDisplay.getArticleHTML(article)).append("</a></td>");
			writer.append("</tr>");
		}
		writer.close();
		System.out.println("Finish generating article detail page.");
	}
	
	public void genAuthorPage(List<String> authors, String save) throws IOException, SQLException{
		System.out.println("Generating author content ...");
		FileWriter writer = new FileWriter(save);
		for(String authorityId: authors){
			writer.append("<tr>");
			writer.append("<td>");
			writer.append("<a href=\'").append("lists/").append(TOPIC+"_author_details.html#"+authorityId+"\'>");
			writer.append(authorDSBService.getAuthorNameByAuthorityId(authorityId)).append("</a>");
			writer.append("</td>");
			writer.append(getChoiceSnippet(authorityId));
			writer.append("</tr>");
		}
		writer.close();
		System.out.println("Finish generating author content.");
	}
	
	public void genAuthorDetailPage(List<String> authors, String save) throws IOException, SQLException{
		System.out.println("Generating author detail page ...");
		FileWriter writer = new FileWriter(save);
		for(String authorityId: authors){
			writer.append("<tr>");
			writer.append("<td>");
			writer.append("<a name=\""+authorityId+"\">");
			String display = resultDisplay.readAuthorFromAuthorityToHTML(authorityId);
			writer.append(display);
			writer.append("</td>");
			writer.append("</tr>");
		}
		writer.close();
		System.out.println("Finish generating author detail page.");
	}
	
	private static String getChoiceSnippet(String name){
		StringBuffer sb = new StringBuffer();
		sb.append("<td><input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"-2\" />-2</td>");
		
		sb.append("<td><input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"-1\" />-1</td>");
		
		sb.append("<td><input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"0\" />0</td>");
		
		sb.append("<td><input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"1\" />1</td>");
		
		sb.append("<td><input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"2\" />2</td>");
		return sb.toString();
	}
	
	private static String getChoiceSnippetSimple(String name){
		StringBuffer sb = new StringBuffer();
		sb.append("<tr><td><input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"-2\" />-2");
		
		sb.append("<input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"-1\" />-1");
		
		sb.append("<input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"0\" />0");
		
		sb.append("<input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"1\" />1");
		
		sb.append("<input type=\"radio\" name=\"");
		sb.append(name);
		sb.append("\" value=\"2\" />2</td></tr>");
		return sb.toString();
	}
	
	public void generateArticleListHTMLContent() throws IOException, SQLException{
		System.out.println("Generating article content ...");
		Set<String> idStrs = IOUtils.readLineAsStringSet(DATA_ROOT+TOPIC+"article_ids.txt");
		System.out.println("article id size:"+idStrs.size());
		List<Integer> idList = new ArrayList<Integer>();
		for(String idStr: idStrs){
			idList.add(Integer.parseInt(idStr));
		}
		List<Article> articles = articleService.getArticleListByArticleIdList(idList);
		genArticlePage(articles, SAVE_ROOT+TOPIC+"_article.html");
		genArticleDetailPage(articles, SAVE_ROOT+TOPIC+"_article_details.html");
		System.out.println("Finish generating article content.");
		
	}
	
	public void generateAuthorListHTMLContent() throws IOException, SQLException{
		Set<String> idStrs = IOUtils.readLineAsStringSet(DATA_ROOT+TOPIC+"author_ids.txt");
		System.out.println("author id size:"+idStrs.size());
		List<String> idList = new ArrayList<String>();
		for(String idStr: idStrs){
			idList.add(idStr);
		}
		genAuthorPage(idList, SAVE_ROOT+TOPIC+"_author.html");
		genAuthorDetailPage(idList, SAVE_ROOT+TOPIC+"_author_details.html");
	}

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		if(args.length != 4){
			System.out.println("--data-root --save-root --topic --type(article/author)");
			return;
		}
		HTMLContent content = new HTMLContent();
		HTMLContent.DATA_ROOT = args[0];
		HTMLContent.SAVE_ROOT = args[1];
		HTMLContent.TOPIC = args[2];
		if(args[3].equals("article")){
			content.generateArticleListHTMLContent();
		}else if(args[3].equals("author")){
			content.generateAuthorListHTMLContent();
		}
	}
}
