/**
 * 
 */
package edu.uwm.elsevier;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.AuthorDAOJdbcImpl;
import articlesdata.article.AuthorService;
import articlesdata.article.Citation;
import articlesdata.citation.Author;
import articlesdata.citation.CitationParserUtils;
import articlesdata.citation.CitationService;

/**
 * @author qing
 *
 */
public class CitationArticleComparison {
	
	public static int EQUAL = 1;
	public static int UNEQUAL = 0;
	public static int CANNOT_COMPARE = 2;
	public static String[] journalNameStopWords = new String[]{"the", "of", "in", "at", "and", "with"};
	private static HashSet<String> journalNameStopSet = null;
	
	
	
	public static HashSet<String> getJournalNameStopSet(){
		if(journalNameStopSet ==null){
			journalNameStopSet = new HashSet<String>();
			for(String word: journalNameStopWords){
				journalNameStopSet.add(word);
			}
		}
		return journalNameStopSet;
	}

	public static void isCitationAndArticleEqual(Citation citation, Article article, List<articlesdata.article.Author> authorsA, MappingStatus status) throws ClassNotFoundException, SQLException{
		
		
		List<Author> authorsC = citation.getAuthors();
		if(authorsC ==null || authorsA==null)
			status.setAuthorsComp(CANNOT_COMPARE);
		else
			status.setAuthorsComp(isCitationArticleAuthorListEqual(authorsC, authorsA)?EQUAL:UNEQUAL);
		
		if(citation.getTitle() ==null || article.getTitle()==null)
			status.setTitleComp(CANNOT_COMPARE);
		else
			status.setTitleComp(isTitleEqual(citation.getTitle(), article.getTitle())?EQUAL:UNEQUAL);
		
		if(citation.getSource()==null || article.getJournal()==null)
			status.setJournalComp(CANNOT_COMPARE);
		else
			status.setJournalComp(isJournalEqualByInitials(citation.getSource(), article.getJournal())?EQUAL:UNEQUAL);
		if(status.isEqual())
			status.setIsMatched(1);
		else
			status.setIsMatched(0);
	}
	
	public static void isCitationAndCitationEqual(Citation c1, Citation c2, MappingStatus status){
		if(c1.getAuthors()== null || c2.getAuthors() == null)
			status.setAuthorsComp(CANNOT_COMPARE);
		else
			status.setAuthorsComp(isCitationCitationAuthorListEqual(c1.getAuthors(), c2.getAuthors())?EQUAL : UNEQUAL);
		
		if(c1.getTitle() == null || c2.getTitle()== null)
			status.setTitleComp(CANNOT_COMPARE);
		else
			status.setTitleComp(isTitleEqual(c1.getTitle(), c2.getTitle())?EQUAL: UNEQUAL);
		if(c1.getSource() == null || c2.getSource()== null)
			status.setJournalComp(CANNOT_COMPARE);
		else
			status.setJournalComp(isJournalEqualByInitials(c1.getSource(), c2.getSource())?EQUAL: UNEQUAL);
		if(status.isEqual())
			status.setIsMatched(1);
		else
			status.setIsMatched(0);
	}
	
	
	public static boolean isCitationArticleAuthorListEqual(List<articlesdata.citation.Author> authorsC, List<articlesdata.article.Author> authorsA){
		if(authorsC.size() != authorsA.size()){	// if number of authors don't match, not equal
			return false;
		}
		for(int i=0; i<authorsC.size(); i++){
			boolean matched = false;
			for( int j=0; j<authorsA.size(); j++){
				if(isCitationArticleAuthorEqual(authorsC.get(i), authorsA.get(j), false)){
					matched = true;
					break;
				}
			}
			if(!matched)	//if one of the authorsC is not matched to any one of authorsA, the author list is not equal.
				return false;
		}
		return true;
	}
	
	
	public static boolean isCitationCitationAuthorListEqual(List<Author> a1, List<Author> a2){
		if(a1.size() != a2.size())
			return false;
		for(int i=0; i<a1.size(); i++){
			boolean matched = false;
			for( int j=0; j<a2.size(); j++){
				if(isSurnamesEqual(a1.get(i).getSurname(), a2.get(j).getSurname())){
					matched = true;
					break;
				}
			}
			if(!matched)	//if one of the authors1 is not matched to any one of authors2, the author list is not equal.
				return false;
		}
		return true;
	}
	
	public static boolean isArticleArticleAuthorListEqual(List<articlesdata.article.Author> a1, List<articlesdata.article.Author> a2){
		if(a1.size()!= a2.size())
			return false;
		for(int i=0; i<a1.size(); i++){
			boolean matched = false;
			for( int j=0; j<a2.size(); j++){
				if(isSurnamesEqual(a1.get(i).getLastName(), a2.get(j).getLastName())){
					matched = true;
					break;
				}
			}
			if(!matched)	//if one of the authors1 is not matched to any one of authors2, the author list is not equal.
				return false;
		}
		return true;
	}
	
	/**
	 * compare an author from a citation and one from article. Case-sensitive!
	 * @param authorC author object from citation (defined by Qing)
	 * @param authorA author object from article (defined by Shashank)
	 * @return
	 */
	public static boolean isCitationArticleAuthorEqual(Author authorC,articlesdata.article.Author authorA, boolean compareGivenNames){
		String surnameC = authorC.getSurname();
		String surnameA = authorA.getLastName();
		boolean isSurnameEqual = false;
		boolean isGivenNameEqual = true;
		if(surnameC !=null && surnameA !=null){	// compare surnames
			isSurnameEqual = surnameC.trim().equals(surnameA.trim());
		}
		if(isSurnameEqual && compareGivenNames){	// compare given names. ignore order here.
			ArrayList<String> givenCs = authorC.getGivenName();
			ArrayList<String> detailedGivenCs = CitationParserUtils.tokenizeGiveNames(givenCs);
			String givenA = authorA.getFirstName();
			int matched = 0;
			if(detailedGivenCs !=null && givenA !=null){
				for(String givenC: detailedGivenCs){
					if(givenA.trim().indexOf(givenC.trim())!= -1){
						matched++;
					}
				}
			}
			if(matched < detailedGivenCs.size()*0.5)
				isGivenNameEqual = false;
		}
		return isSurnameEqual && isGivenNameEqual;
	}
	
	
	public static boolean isSurnamesEqual(String surname1, String surname2){
		
		boolean isSurnameEqual = false;
		if(surname1 !=null && surname2 !=null){	// compare surnames
			isSurnameEqual = surname1.trim().equals(surname2.trim());
		}
		return isSurnameEqual;
	}
	
	public static boolean isDateEqual(int dateC, String dateAStr){
		int dateA = CitationParserUtils.getYear(dateAStr);
		return dateC == dateA;
	}
	
	public static boolean isEqual(MappingStatus status){
		if(status.getTitleComp()==EQUAL && status.getAuthorsComp()==EQUAL)
			return true;
		else if(status.getAuthorsComp()==EQUAL && status.getJournalComp()==EQUAL)
			return true;
		else if(status.getTitleComp() == EQUAL && status.getJournalComp()== EQUAL)
			return true;
		else 
			return false;
	}
	/**
	 * MAY HAVE CODING PROBLEM HERE. MAY NEED TO ENFORCE UTF8
	 * @param titleC
	 * @param titleA
	 * @return
	 */
	public static boolean isTitleEqual(String titleC, String titleA){
		String regex = "[\\s\\?!,.:]";
		titleC = StringEscapeUtils.unescapeHtml(titleC).toLowerCase();
		titleA = StringEscapeUtils.unescapeHtml(titleA).toLowerCase();
		ArrayList<String> wordsC = CitationParserUtils.simpleTokenization(titleC, regex);
		ArrayList<String> wordsA = CitationParserUtils.simpleTokenization(titleA, regex);
		HashSet<String> setA = new HashSet<String>(wordsA);
		int matched = 0;
		for(String wordC : wordsC){
			if(setA.contains(wordC))
				matched++;
		}
		if(matched == wordsC.size() || matched == wordsA.size())	// if fully matched for one source, they are considered equal.
			return true;
		if(matched >= wordsC.size()*0.8 && matched >= wordsA.size()*0.8)
			return true;
		return false;
	}
	
	public static boolean isJournalEqualByInitials(String journalC, String journalA){
		if(journalC == null || journalA == null)
			return true;
		String regex = "[\\s\\?!,.:<>\\(\\)&]";
		List<String> wordsC = tokenizationAndRemoveStopWords(journalC, regex);
		List<String> wordsA = tokenizationAndRemoveStopWords(journalA, regex);
		HashSet<Character> charC = new HashSet<Character>(); 
		for(String word : wordsC){
			charC.add(word.charAt(0));
		}
		int matched = 0;
		for(String word: wordsA){
			if(charC.contains(word.charAt(0)))
				matched++;
		}
		
		if(matched >= wordsC.size()*0.8 && matched >=wordsA.size()*0.8)
			return true;
		return false;
	}
	
	public static List<String> tokenizationAndRemoveStopWords(String str, String regex){
		str =StringEscapeUtils.unescapeHtml(str);
		ArrayList<String> words = CitationParserUtils.simpleTokenization(str, regex);
		List<String> result = new ArrayList<String>();
		for(String word : words){
			if(word.length() >0 && !getJournalNameStopSet().contains(word))
				result.add(word);
		}
		words.clear();
//		System.out.println(result.toString());
		return result;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String c= "Science";
//		String a = "Social Science Research";
//		int articleId = 0;
//		long citationId = 0;
//		try {
//			ArticleService articleService = new ArticleService();
//			CitationService citationService = new CitationService();
//			AuthorService authorService = new AuthorService();
//			Article article = articleService.getArticleByArticleId(articleId);
//			Citation citation = citationService.getCitationByCitationId(citationId);
//			List<articlesdata.article.Author> authorsA = authorService.getAuthorListByArticleId(articleId);
//			MappingStatus status = new MappingStatus(citationId, articleId);
//			isCitationAndArticleEqual(citation, article, authorsA, status);
//			System.out.println("title\t"+status.getTitleComp()
//					+"author\t"+status.getAuthorsComp()
//					+"journal\t"+status.getJournalComp());
//			
//			
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		boolean r = CitationArticleComparison.isJournalEqualByInitials(c, a);
//		System.out.println(r);
		
		String j1 = "Prog. Neuropsychopharmacol. Biol. Psychiatry";
		String j2 = "Progress in Neuropsychopharmacology &amp; Biological Psychiatry";
		System.out.println(CitationArticleComparison.isJournalEqualByInitials(j1, j2));
		
	}

}
