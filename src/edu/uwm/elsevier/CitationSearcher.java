/**
 * 
 */
package edu.uwm.elsevier;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Version;

import edu.uwm.elsevier.indexer.ElsevierCitationIndexer;
import edu.uwm.elsevier.indexer.ElsevierIndexer;

import articlesdata.article.Article;
import articlesdata.article.Author;
import articlesdata.article.Citation;
import articlesdata.citation.CitationService;

/**
 * @author qing
 *
 */
public class CitationSearcher extends ElsevierArticleMetaDataSearcher {

	protected HashMap<Long, List<articlesdata.article.Author>> citationIdAuthorMap;
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("CitationSearcher");
	/**
	 * @param indexPath
	 * @throws IOException
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public CitationSearcher(String indexPath) throws IOException, ClassNotFoundException, SQLException {
		super(indexPath);
		NUM_OF_SEARCH_RESTURN = 4000;
	}
	
	public List<Citation> getCitations(Citation c) throws IOException{
		BooleanQuery query = getQuery(c.getTitle(), c.getAuthors());
		List<Citation> citations = new ArrayList<Citation>();
		citationIdAuthorMap = new HashMap<Long, List<articlesdata.article.Author>>();
		if(query == null){
			return citations;
		}
		if(c.getTitle() !=null)
			LOGGER.debug(query.toString());
		TopDocs hits = searcher.search(query, NUM_OF_SEARCH_RESTURN);
		ScoreDoc[] docs = hits.scoreDocs;
		if(docs!=null ){
			for(ScoreDoc d: docs){
				Document document = searcher.getIndexReader().document(d.doc);
				String citationId = document.get(ElsevierCitationIndexer.CITATION_ID_FIELD);// not null
				String articleId = document.get(ElsevierIndexer.ARTICLE_ID_FIELD);
				String title = document.get(ElsevierIndexer.TITLE_FIELD);
				String journal = document.get(ElsevierIndexer.JOURNAL_FIELD);
				String authorsStr = document.get(ElsevierIndexer.AUTHORS_FIELD);
				String citationKey = document.get(ElsevierCitationIndexer.CITATION_KEY_FIELD);
				String citationText = document.get(ElsevierCitationIndexer.CITATION_TEXT_FIELD);
				Citation citation = new Citation();
				citation.setCitationId(Long.parseLong(citationId));
				citation.setArticleId(Integer.parseInt(articleId));
				citation.setTitle(title.length()==0? null: title);
				citation.setSource(journal.length()==0?null: journal);
				citation.setCitationKey(citationKey.length()==0? null: citationKey);
				citation.setCitationText(citationText.length()==0?null: citationText);
				List<articlesdata.citation.Author> authors = parseAuthorFieldIntoCitationAuthors(authorsStr);
				citation.setAuthors(authors);
				citations.add(citation);
			}
		}
		return citations;
	}
	
	public List<Citation> getCitationsByQuery(Query query) throws IOException, ParseException{
//		QueryParser parser = new QueryParser(Version.LUCENE_35, "title", new StandardAnalyzer(Version.LUCENE_35));
		List<Citation> citations = new ArrayList<Citation>();
		citationIdAuthorMap = new HashMap<Long, List<articlesdata.article.Author>>();
		if(query == null){
			return citations;
		}
//		Query q = parser.parse(query.toString());
		System.out.println(query.toString());
		TopDocs hits = searcher.search(query, NUM_OF_SEARCH_RESTURN);
		ScoreDoc[] docs = hits.scoreDocs;
		if(docs!=null ){
			for(ScoreDoc d: docs){
				Document document = searcher.getIndexReader().document(d.doc);
				String citationId = document.get(ElsevierCitationIndexer.CITATION_ID_FIELD);// not null
				String articleId = document.get(ElsevierIndexer.ARTICLE_ID_FIELD);
				String title = document.get(ElsevierIndexer.TITLE_FIELD);
				String journal = document.get(ElsevierIndexer.JOURNAL_FIELD);
				String authorsStr = document.get(ElsevierIndexer.AUTHORS_FIELD);
				String citationKey = document.get(ElsevierCitationIndexer.CITATION_KEY_FIELD);
				String citationText = document.get(ElsevierCitationIndexer.CITATION_TEXT_FIELD);
				Citation citation = new Citation();
				citation.setCitationId(Long.parseLong(citationId));
				citation.setArticleId(Integer.parseInt(articleId));
				citation.setTitle(title.length()==0? null: title);
				citation.setSource(journal.length()==0?null: journal);
				citation.setCitationKey(citationKey.length()==0? null: citationKey);
				citation.setCitationText(citationText.length()==0?null: citationText);
				List<articlesdata.citation.Author> authors = parseAuthorFieldIntoCitationAuthors(authorsStr);
				citation.setAuthors(authors);
				citations.add(citation);
			}
		}
		return citations;
	}

	public HashMap<Long, List<articlesdata.article.Author>> getCitationIdAuthorMap() {
		return citationIdAuthorMap;
	}
	
	public static void main(String[] args){
		if(args.length != 1){
			System.out.println("--citation-id");
			return;
		}
		long citationId = Long.parseLong(args[0]);
		BooleanQuery query = new BooleanQuery();
		query.add(new TermQuery(new Term(ElsevierIndexer.AUTHORS_FIELD, "Bender")), Occur.SHOULD);
		query.add(new TermQuery(new Term(ElsevierIndexer.AUTHORS_FIELD, "Brody")), Occur.SHOULD);
		query.add(new TermQuery(new Term(ElsevierIndexer.AUTHORS_FIELD, "Chen")), Occur.SHOULD);
		try {
			CitationService citationService = new CitationService();
			Citation c = citationService.getCitationByCitationId(citationId);
			CitationSearcher searcher = new CitationSearcher("/Users/qing/wind3/elsevier_citation_index2");
			List<Citation> citations = searcher.getCitations(c);
			int i=0;
			for(Citation citation: citations){
				System.out.println(i+"\n"+citation.toString());
				i++;
				if(i> 10)
					break;
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

}
