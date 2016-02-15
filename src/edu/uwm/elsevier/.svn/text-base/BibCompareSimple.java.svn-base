/**
 * 
 */
package edu.uwm.elsevier;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.article.Citation;
import articlesdata.citation.CitationParserUtils;
import articlesdata.citation.CitationService;

/**
 * @author qing
 *
 */
public class BibCompareSimple {

	
	public static void isRawCitationTextAndArticleEqual(String citationText, Article article, List<articlesdata.article.Author> authorsA, MappingStatus status){
		if(citationText ==null || authorsA == null)
			status.setAuthorsComp(CitationArticleComparison.CANNOT_COMPARE);
		else
			status.setAuthorsComp(isAuthorListEqual(citationText, authorsA)?CitationArticleComparison.EQUAL: CitationArticleComparison.UNEQUAL);
		if(citationText ==null || article.getTitle() == null)
			status.setTitleComp(CitationArticleComparison.CANNOT_COMPARE);
		else
			status.setTitleComp(isTitleEqual(citationText, article.getTitle())?CitationArticleComparison.EQUAL: CitationArticleComparison.UNEQUAL);
		if(citationText ==null || article.getJournal() == null)
			status.setJournalComp(CitationArticleComparison.CANNOT_COMPARE);
		else
			status.setJournalComp(isJournalEqual(citationText, article.getJournal())?CitationArticleComparison.EQUAL: CitationArticleComparison.UNEQUAL);
	
	}
	
	public static boolean isAuthorListEqual(String citationText, List<articlesdata.article.Author> authors){
		
		for(articlesdata.article.Author author: authors){
			if(citationText.indexOf(author.getLastName())==-1)
				return false;
		}
		return true;
	}
	
	public static boolean isTitleEqual(String citationText, String title){
		String regex = "[\\s\\?!,.:<>\\(\\)&]";
		citationText = StringEscapeUtils.unescapeHtml(citationText);
		title = StringEscapeUtils.unescapeHtml(title);
		ArrayList<String> wordsC = CitationParserUtils.simpleTokenization(citationText, regex);
		ArrayList<String> wordsA = CitationParserUtils.simpleTokenization(title, regex);
		HashSet<String> setA = new HashSet<String>(wordsA);
		int matched = 0;
		for(String wordC : wordsC){
			if(setA.contains(wordC))
				matched++;
		}
		if(matched >= wordsA.size()*0.8)
			return true;
		return false;
	}
	
	public static boolean isJournalEqual(String citationText, String journal){
		String regex = "[\\s\\?!,.:<>\\(\\)&]";
		List<String> wordsC = CitationArticleComparison.tokenizationAndRemoveStopWords(citationText, regex);
		List<String> wordsA = CitationArticleComparison.tokenizationAndRemoveStopWords(journal, regex);
		int matched = 0;
		for(String word: wordsA){
			for(String c: wordsC){
				if(c.startsWith(word)|| word.startsWith(c))
					matched ++;
			}
		}
		if(matched ==wordsA.size())
			return true;
		return false;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long citationId = 5878253;
		int articleId = 1983;
		try {
			ArticleService articleService = new ArticleService();
			AuthorService authorService = new AuthorService();
			CitationService citationService = new CitationService();
			Article article = articleService.getArticleByArticleId(articleId);
			List<Author> authors = authorService.getAuthorListByArticleId(articleId);
			Citation citation = citationService.getCitationByCitationId(citationId);
			MappingStatus status = new MappingStatus(citationId, articleId);
			isRawCitationTextAndArticleEqual(citation.getCitationText(), article, authors, status);
			status.setIsMatched(status.isEqual()?1:0);
			System.out.println(status.toString());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
