/**
 * 
 */
package pmidmapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import articlesdata.article.Article;
import articlesdata.citation.Author;

import edu.uwm.elsevier.ElsevierArticleMetaDataSearcher;


/**
 * @author qing
 *
 */
public class MedlineSearcher {

	public static String defaultMedlineIndexPath = "/home/data_user/pubmed_index";
	private IndexSearcher searcher;
	private IndexReader reader ;
	private int numDocs =0;
	public static int NUM_OF_SEARCH_RESTURN = 10;
	protected QueryParser parser = new QueryParser(Version.LUCENE_35, "id", new StandardAnalyzer(Version.LUCENE_35));
	
	public MedlineSearcher(String indexPath) throws IOException{
		NIOFSDirectory directory;
		directory = new NIOFSDirectory(new File(indexPath));
		reader = IndexReader.open(directory, true);
		searcher = new IndexSearcher(reader);
		numDocs = reader.numDocs();
	}
	
	public List<articlesdata.article.Author> parsePMAuthors(String authorStrs){
		List<articlesdata.article.Author> authors  = new ArrayList<articlesdata.article.Author>();
		if(authorStrs == null)
			return authors;
		String[] names = authorStrs.split(";");
		for(String name: names){
			if(name.trim().length()!=0){
				articlesdata.article.Author a = new articlesdata.article.Author();
				a.setLastName(name.trim());// consider the whole name as last name
				authors.add(a);
			}
		}
		return authors;
	}
	
	public static List<String> parseMeshs(String meshStr){
		List<String> meshList =new ArrayList<String>();
		if(meshStr == null)
			return meshList;
		String[] meshs = meshStr.split(";");
		for(String mesh: meshs){
			if(mesh.trim().length() !=0){
				meshList.add(mesh.trim());
			}
		}
		return meshList;
	}
	
	public static String normalizeMesh(String mesh){
		
		String[] words = mesh.toLowerCase().split("[\\s,]+");
		StringBuffer sb =new StringBuffer();
		for(String word: words){
			sb.append(word).append("#");
		}
		return sb.toString();
	}
	
	public List<PMArticle> getPMArticlesByQuery(String title, List<Author> authors) throws IOException{
		 BooleanQuery query = ElsevierArticleMetaDataSearcher.getQuery(title, authors);
		 return getPMArticlesByQuery(query);
	}
	
	public  List<PMArticle> getPMArticlesByQuery(Query query) throws IOException{
		List<PMArticle> pmArticles = new ArrayList<PMArticle>();
		 if(query == null){
			 return pmArticles;
		 }
		 TopDocs hits  = searcher.search(query, NUM_OF_SEARCH_RESTURN);
		 ScoreDoc[] docs = hits.scoreDocs;
		 if(docs!= null){
			 for(int i=0; i<docs.length; i++){
				 Document document = searcher.getIndexReader().document(docs[i].doc);
				 String pmidStr = document.get("id");
				 if(pmidStr == null)
					 continue;
				 String pmTitle = document.get("title");
				 String pmJournal = document.get("journalTitle");
				 String pmAuthors = document.get("authors");
				 String pmYear = document.get("year");
				 String meshs = document.get("mesh");
				 
				PMArticle article = new PMArticle();
				article.setPmid(Long.parseLong(pmidStr));
				article.setTitle(pmTitle);
				article.setJournal(pmJournal);
				article.setAuthorList(parsePMAuthors(pmAuthors));
				article.setYear(pmYear);
				article.setMeshs(meshs);
				pmArticles.add(article);
			 }
		 }
		 return pmArticles;
	}
	
	public PMArticle getPMArticleByPMID(long pmid, boolean withAbstract) throws ParseException, IOException{
		Query query = parser.parse("id:"+pmid);
		TopDocs hits = searcher.search(query, 1);
		ScoreDoc[] docs = hits.scoreDocs;
		 if(docs!= null && docs.length >0){
			 Document document = searcher.getIndexReader().document(docs[0].doc);
			 PMArticle article = new PMArticle();
			 long pmidReturn = Long.parseLong(document.get("id"));
			 article.setPmid(pmidReturn);
			 article.setTitle(document.get("title"));
			 article.setJournal(document.get("journalTitle"));
			 article.setAuthorList(parsePMAuthors(document.get("authors")));
			 article.setMeshs(document.get("mesh"));
			 article.setYear(document.get("year"));
			 if(withAbstract)
				 article.setAbs(document.get("abstract"));
			 return article;
		 }
		 return null;
	}
	
	public TermFreqVector[]  getTermVec(long pmid) throws ParseException, IOException{
		String queryStr = "id:"+pmid;
		Query query = parser.parse(queryStr);
		TopDocs topDocs = searcher.search(query, 1);
		ScoreDoc[] docs = topDocs.scoreDocs;
		TermFreqVector[] termFreqs =null;
		if(docs.length==1){
			termFreqs = reader.getTermFreqVectors(docs[0].doc);
		}
		return termFreqs;
	}
	
	public Map<String, Float> getIDFForField(Set<String> terms, String field) throws IOException{
		Map<String, Float> idfMap = new HashMap<String, Float>();
		for(String term: terms){
			float idf = DefaultSimilarity.getDefault().idf(reader.docFreq(new Term(field, term)), numDocs);
			idfMap.put(term, idf);
		}
		return idfMap;
	}
	
	
	public void close() throws IOException{
		if(searcher !=null){
			searcher.close();
			reader.close();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//			MedlineSearcher searcher = new MedlineSearcher(MedlineSearcher.defaultMedlineIndexPath);
		String mesh = "food service, hospital";
		String normed = MedlineSearcher.normalizeMesh(mesh);
		System.out.println(normed);
	}

}
