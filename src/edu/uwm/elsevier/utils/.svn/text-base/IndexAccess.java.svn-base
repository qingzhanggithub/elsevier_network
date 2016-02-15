/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;


import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.indexer.ElsevierIndexer;

/**
 * @author qing
 *
 */
public class IndexAccess {

	private IndexReader reader ;
	private IndexSearcher searcher;
	private QueryParser parser; 
	private int numDocs =0;
	public int NUM_OF_SEARCH_RESTURN = 20000;
	private CitationNetworkService citationNetworkService;
	public IndexAccess() throws IOException, ClassNotFoundException, SQLException{
		NIOFSDirectory directory;
		directory = new NIOFSDirectory(new File(ElsevierIndexer.INDEX_PATH));
		reader = IndexReader.open(directory, true);
		numDocs = reader.numDocs();
		searcher = new IndexSearcher(reader);
		parser = new QueryParser(Version.LUCENE_35,"content",new StandardAnalyzer(Version.LUCENE_35));
		citationNetworkService = new CitationNetworkService();
	}
	
	
	public List<String> searchIndex(Query query, int numOfReturn) throws IOException, NumberFormatException, SQLException{
		TopDocs hits  = searcher.search(query, numOfReturn);
		ScoreDoc[] docs = hits.scoreDocs;
		List<String> articleIdList = new ArrayList<String>();
		if(docs!=null ){
			for(int i=0; i<docs.length; i++){
				Document document = searcher.getIndexReader().document(docs[i].doc);
				String articleIdStr = document.get(ElsevierIndexer.ARTICLE_ID_FIELD);
				if(articleIdStr !=null){
//					 long pmid = citationNetworkService.getPMIDByArticleId(Integer.parseInt(articleIdStr));
					articleIdList.add(articleIdStr);
				}
			}
		}
		return articleIdList;
	}
	
	
	public TermFreqVector[]  getTermVec(int articleId) throws ParseException, IOException{
		String queryStr = "article_id:"+articleId;
		Query query = parser.parse(queryStr);
		TopDocs topDocs = searcher.search(query, 1);
		ScoreDoc[] docs = topDocs.scoreDocs;
		TermFreqVector[] termFreqs =null;
		if(docs.length==1){
			termFreqs = reader.getTermFreqVectors(docs[0].doc);
		}
		return termFreqs;
	}
	
	
	public List<float[]> getIDFForAll(TermFreqVector[] vecs) throws IOException{ 
		List<float[]> idfList = new ArrayList<float[]>();
		for(TermFreqVector vec: vecs){
			String[] terms = vec.getTerms();
			String field = vec.getField();
			float[] idfs = new float[terms.length];
			for(int i=0; i<terms.length; i++){
				float idf = DefaultSimilarity.getDefault().idf(reader.docFreq(new Term(field, terms[i])), numDocs);
				idfs[i] = idf;
			}
			idfList.add(idfs);
		}
		return idfList;
	}
	
	public float[] getIDFForField(String[] terms, String field) throws IOException{
		float[] idfs = new float[terms.length];
		for(int i=0; i< terms.length; i++){
			float idf = DefaultSimilarity.getDefault().idf(reader.docFreq(new Term(field, terms[i])), numDocs);
			idfs[i] = idf;
		}
		return idfs;
	}
	
	public Map<String, Float> getIDFForField(Set<String> terms, String field) throws IOException{
		Map<String, Float> idfMap = new HashMap<String, Float>();
		for(String term: terms){
			float idf = DefaultSimilarity.getDefault().idf(reader.docFreq(new Term(field, term)), numDocs);
			idfMap.put(term, idf);
		}
		return idfMap;
	}
	
	public int getNumDocs() {
		return numDocs;
	}

	public IndexReader getReader() {
		return reader;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
