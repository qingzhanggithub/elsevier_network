/**
 * 
 */
package edu.uwm.elsevier.linkanalysis.matrix;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;

import edu.uwm.elsevier.ElsevierArticleMetaDataSearcher;
import edu.uwm.elsevier.indexer.ElsevierIndexer;
import edu.uwm.elsevier.utils.IndexAccess;
import articlesdata.citation.CitationParserUtils;

/**
 * @author qing
 *
 */
public class Searcher {

	public static BooleanQuery getAndQuery(String text){
		BooleanQuery query = new BooleanQuery();
		ArrayList<String> tokens = CitationParserUtils.simpleTokenization(text, ElsevierArticleMetaDataSearcher.TEXT_TOKENIZE_PATTERN);
		for(String token: tokens){
			query.add(new TermQuery(new Term(ElsevierIndexer.BODY_FIELD, token)), Occur.MUST);
		}
		return query;
	}
	
	public static void searchIndex(String queryStr, int numOfReturn, String save) throws IOException, ClassNotFoundException, SQLException{
		BooleanQuery query = getAndQuery(queryStr);
		IndexAccess ia = new IndexAccess();
		System.out.println("Querying");
		List<String> articleIdStrs = ia.searchIndex(query, numOfReturn);
		FileWriter writer = new FileWriter(save);
		for(String id: articleIdStrs){
			writer.append(id).append('\n');
		}
		writer.close();
		System.out.println("Task done.");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length != 3){
			System.out.println("--query --num-of-return --save");
			return;
		}
		try {
			Searcher.searchIndex(args[0], Integer.parseInt(args[1]), args[2]);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
