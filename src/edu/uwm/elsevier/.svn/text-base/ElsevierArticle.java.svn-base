/**
 * 
 */
package edu.uwm.elsevier;

import java.util.List;

import articlesdata.article.Article;
import articlesdata.article.Author;

/**
 * @author qing
 *
 */
public class ElsevierArticle extends Article {
	private List<Author> authors;
	
	public ElsevierArticle(Article article){
		title = article.getTitle();
		journal = article.getJournal();
		identifier = article.getIdentifier();
		articleId = article.getArticleId();
		sourceId = article.getSourceId() ;
		issue = article.getIssue() ;
		volume = article.getVolume();
		datePublished = article.getDatePublished();
	}
	
	public ElsevierArticle(){
		
	}

	public List<Author> getAuthors() {
		return authors;
	}

	public void setAuthors(List<Author> authors) {
		this.authors = authors;
	}
	
	
	
}
