/**
 * 
 */
package edu.uwm.elsevier.evaluation;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.lang.StringEscapeUtils;

import articlesdata.citation.CitationParserUtils;
import edu.uwm.elsevier.CitationArticleComparison;

/**
 * @author qing
 *
 */
public class FuzzyComparison {

	public static boolean isTitleFuzzyEqual(String titleC, String titleA, float portion){
		if(titleC == null || titleA == null)
			return false;
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
		if(matched >= wordsC.size()*portion && matched >= wordsA.size()*portion)
			return true;
		return false;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
