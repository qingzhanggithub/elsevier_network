/**
 * 
 */
package edu.uwm.elsevier.indexer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
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

import edu.uwm.elsevier.authoranalysis.InterDisipline;

/**
 * @author qing
 *
 */
public class ElsevierCitationIndexAccess {
	
	private IndexReader reader ;
	private IndexSearcher searcher;
	private QueryParser parser; 
	private int numDocs =0;
	
	public ElsevierCitationIndexAccess(String defaultField) throws IOException{
		NIOFSDirectory directory;
		directory = new NIOFSDirectory(new File(ElsevierCitationIndexer.ELSEVIER_CITATION_INDEX_PATH));
		reader = IndexReader.open(directory, true);
		numDocs = reader.numDocs();
		searcher = new IndexSearcher(reader);
		parser = new QueryParser(Version.LUCENE_35, defaultField, new StandardAnalyzer(Version.LUCENE_35));
	}
	
	public TermFreqVector[]  getTermVec(String queryStr) throws ParseException, IOException{
		Query query = parser.parse(queryStr);
		TopDocs topDocs = searcher.search(query, 10);
		ScoreDoc[] docs = topDocs.scoreDocs;
		TermFreqVector[] termFreqs =null;
		if(docs.length >0){
			termFreqs = reader.getTermFreqVectors(docs[0].doc);
		}
		return termFreqs;
	}
	
	
	public Map<String, Float> getTFIDFForCitationList(List<Long> citationIds) throws ParseException, IOException{
		List<TermFreqVector[]> termVecList = new ArrayList<TermFreqVector[]>();
		for(long citationId: citationIds){
			TermFreqVector[] termVec = getTermVec(ElsevierCitationIndexer.CITATION_ID_FIELD+":"+citationId);
			if(termVec !=null){
				termVecList.add(termVec);
//				System.out.println("Found term vector for citation id "+citationId);
			}
			else
				System.err.println("No index for citation id "+citationId);
		}
		
		if(termVecList.size() == 0){
			System.err.println("No citation found for the citation id list.");
			return null;
		}
		
		List<Map<String, TermFreqVector>> mapList = new ArrayList<Map<String,TermFreqVector>>();
		for(int i=0; i< termVecList.size() ; i++){
			TermFreqVector[] termVecs = termVecList.get(i);
			Map<String, TermFreqVector> fieldMap = new HashMap<String, TermFreqVector>();
			for(TermFreqVector vec: termVecs){
				fieldMap.put(vec.getField(), vec);
			}
			mapList.add(fieldMap);
		}
		
		Map<String, Integer> tfMap = InterDisipline.mergeTermVectors(mapList, ElsevierCitationIndexer.CITATION_TEXT_FIELD);
		Map<String, Float> idfMap = getIDFForField(tfMap.keySet(), ElsevierCitationIndexer.CITATION_TEXT_FIELD);
		Map<String, Float> tfidfMap = InterDisipline.getTFIDFMap(tfMap, idfMap);
		
		return tfidfMap;
	}
	
	public Map<String, Float> getIDFForField(Set<String> terms, String field) throws IOException{
		Map<String, Float> idfMap = new HashMap<String, Float>();
		for(String term: terms){
			float idf = DefaultSimilarity.getDefault().idf(reader.docFreq(new Term(field, term)), numDocs);
			idfMap.put(term, idf);
		}
		return idfMap;
	}
	

	public IndexReader getReader() {
		return reader;
	}

	public int getNumDocs() {
		return numDocs;
	}
	
	public void close() throws IOException{
		if( reader!= null)
			reader.close();
		if(searcher !=null)
			searcher.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
