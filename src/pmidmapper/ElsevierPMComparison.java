/**
 * 
 */
package pmidmapper;

import java.util.List;

import articlesdata.article.Article;
import articlesdata.article.Author;
import edu.uwm.elsevier.CitationArticleComparison;
import edu.uwm.elsevier.MappingStatus;

/**
 * @author qing
 *
 */
public class ElsevierPMComparison extends CitationArticleComparison {

	
	public static boolean isAuthorEqualPM(Author elsevier, Author pm){
		if(pm.getLastName().indexOf(elsevier.getLastName())!= -1)
			return true;
		return false;
		
	}
	
	public static boolean isAuthorListEqualPM(List<Author> elsevierAuthors, List<Author> pmAuthors){
		if(elsevierAuthors.size() != pmAuthors.size()){
			return false;
		}
		for(Author els: elsevierAuthors){
			boolean isMatched = false;
			for(Author pm: pmAuthors){
				if(isAuthorEqualPM(els, pm)){
					isMatched = true;
					break;
				}
			}
			if(!isMatched)
				return false;
		}
		return true;
	}
	
	public static void isElsevierAndPMArticleEqual(Article elsevierArticle, List<Author> elseAuthors, PMArticle pmArticle, MappingStatus status){
		List<Author> pmAuthors = pmArticle.getAuthorList();
		if(elseAuthors == null || pmAuthors== null)
			status.setAuthorsComp(CANNOT_COMPARE);
		else
			status.setAuthorsComp(isAuthorListEqualPM(elseAuthors, pmAuthors)?EQUAL:UNEQUAL);
		
		if(elsevierArticle.getTitle()== null || pmArticle.getTitle()== null)
			status.setTitleComp(CANNOT_COMPARE);
		else
			status.setTitleComp(isTitleEqual(elsevierArticle.getTitle(), pmArticle.getTitle())?EQUAL:UNEQUAL);
		
		if(elsevierArticle.getJournal() == null || pmArticle.getJournal() == null)
			status.setJournalComp(CANNOT_COMPARE);
		else
			status.setJournalComp(isJournalEqualByInitials(elsevierArticle.getJournal(), pmArticle.getJournal())?EQUAL:UNEQUAL);
		
		if(status.getTitleComp()==EQUAL && status.getAuthorsComp()==EQUAL)
			status.setIsMatched(1);
		else if(status.getAuthorsComp()==EQUAL && status.getJournalComp()==EQUAL)
			status.setIsMatched(1);
		else if(status.getTitleComp() == EQUAL && status.getJournalComp()== EQUAL)
			status.setIsMatched(1);
		else 
			status.setIsMatched(0);
			
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
