/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

/**
 * @author qing
 *
 */
public class MedlineSearcherForDSB {
	
//	private String indexPath = "/home/data_user/pubmed_index";
	private String indexPath = "/Users/qing/datauser/data_user/pubmed_index";
	private static IndexSearcher searcher = null;
	protected QueryParser parser = new QueryParser(Version.LUCENE_35, "id", new StandardAnalyzer(Version.LUCENE_35));
	
	public MedlineSearcherForDSB() {
		
	}
	
	public IndexSearcher getSearcherInstance() throws IOException{
		if(searcher == null){
			NIOFSDirectory directory;
			directory = new NIOFSDirectory(new File(indexPath));
			IndexReader reader = IndexReader.open(directory, true);
			searcher = new IndexSearcher(reader);
		}
		return searcher;
	}
	
	
	public float[] getTfidfByPMID(int pmid) throws ParseException, IOException{
		String queryStr = "id:"+pmid;
		Query query = parser.parse(queryStr);
		TopDocs hits = getSearcherInstance().search(query, 1);
		ScoreDoc[] docs = hits.scoreDocs;
		IndexReader ireader = getSearcherInstance().getIndexReader();
		int numDocs = ireader.numDocs();
		if(docs !=null && docs.length>0){
			 TermFreqVector termVector = ireader.getTermFreqVector(docs[0].doc, "abstract");
			 if(termVector !=null){
				 String[] terms = termVector.getTerms();
				 int[] freqs = termVector.getTermFrequencies();
				 int size = terms.length;
				 float[] tfidfs = new float[size];
				 for(int i=0; i<size; i++){
					float idf = DefaultSimilarity.getDefault().idf(ireader.docFreq(new Term("abstract", terms[i])), numDocs);
					float tf = DefaultSimilarity.getDefault().tf(freqs[i]);
					float tfidf = tf*idf;
					tfidfs[i] = tfidf;
				 }
				 return tfidfs;
			 }
		}
		return null;
	}
	

	public void close() throws IOException{
		if(searcher !=null)
			searcher.close();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MedlineSearcherForDSB searcher = new MedlineSearcherForDSB();
		try {
			float[] tfidf1 = searcher.getTfidfByPMID(12714306);
			float[] tfidf2 = searcher.getTfidfByPMID(12714319);
			System.out.println("size1="+tfidf1.length+"\tsize2="+tfidf2.length);
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
