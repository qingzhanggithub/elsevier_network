/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.io.IOException;

import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.queryParser.ParseException;

import edu.uwm.elsevier.indexer.ElsevierCitationIndexAccess;
import edu.uwm.elsevier.indexer.ElsevierCitationIndexer;

/**
 * @author qing
 *
 */
public class CitationIndexTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		ElsevierCitationIndexAccess access = new ElsevierCitationIndexAccess(ElsevierCitationIndexer.CITATION_TEXT_FIELD);
		TermFreqVector[] termVec = access.getTermVec(args[0]);
		System.out.println(termVec.length);
	}

}
