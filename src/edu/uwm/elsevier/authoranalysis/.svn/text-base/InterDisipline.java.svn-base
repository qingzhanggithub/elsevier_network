/**
 * 
 */
package edu.uwm.elsevier.authoranalysis;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.queryParser.ParseException;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.utils.IndexAccess;

/**
 * @author qing
 *
 */
public class InterDisipline {

	public static String TEXT_FIELD = "abstract";
	private AuthorDSBService authorDSBService;
	private CitationNetworkService citationNetworkService;
	private IndexAccess indexAccess ;
	private int endYear = 2000;
	private int startYear = -1;
	private static Logger logger = Logger.getLogger(InterDisipline.class);
	public InterDisipline(int endYear) throws ClassNotFoundException, SQLException, IOException{
		authorDSBService = new AuthorDSBService();
		citationNetworkService = new CitationNetworkService();
		indexAccess = new IndexAccess();
		this.endYear = endYear;
	}
	
	public InterDisipline(){
		
	}
	
	public static double getCosine(Map<String, Float> tfidfMapA, Map<String, Float> tfidfMapB){
		if(tfidfMapA == null || tfidfMapB == null){
			logger.info("tfidfmap pairs are null ...");
			return 0;
		}
		Set<String> termSetA = tfidfMapA.keySet();
		float sum = 0f;
		for(String term: termSetA){
			Float tfidfA =tfidfMapA.get(term);
			Float tfidfB =tfidfMapB.get(term);
			if(tfidfA !=null && tfidfB !=null)
				sum += tfidfMapA.get(term) * tfidfMapB.get(term);
		}
		float sqSumA = 0;
		float sqSumB = 0;
		for(String ter: termSetA){
			Float tfidf = tfidfMapA.get(ter);
			sqSumA += tfidf* tfidf;
			
		}
		for(String term: tfidfMapB.keySet()){
			float tfidf = tfidfMapB.get(term);
			sqSumB += tfidf * tfidf;
		}
		
		if(sqSumA==0)
			logger.error("sqSumA ==0");
		if(sqSumB ==0)
			logger.error("sqSumB ==0");
		if(sqSumA !=0 && sqSumB!= 0){
			double cosine = sum/(Math.sqrt(sqSumA)* Math.sqrt(sqSumB));
			return cosine;
		}else
			return 0;
	}
	
	public Map<String, Float>getTFIDFForAuthor(String author) throws SQLException, ParseException, IOException{
		logger.info("Getting tfidf for author:"+author);
		List<Long> pmids = authorDSBService.getPmidsBetweenYearsByAuthorityId(author, startYear, endYear);
		return getTFIDFForPMIDs(pmids);
	}
	
	public Map<String, Float> getTFIDFForPMIDs(List<Long> pmids) throws SQLException, ParseException, IOException{
		logger.info("Getting tfidf for pmids. Number of articles: "+pmids.size());
		List<TermFreqVector[]> termVecList = new ArrayList<TermFreqVector[]>();
		for(long id: pmids){
			List<Integer> articleIdCandidates = citationNetworkService.getArticleIdByPMID(id);
			if(articleIdCandidates.size()==1){
				TermFreqVector[] termVecs = indexAccess.getTermVec(articleIdCandidates.get(0));
				if(termVecs !=null)
					termVecList.add(termVecs);
			}
		}
		if(termVecList.size() ==0){
			logger.info("TermVecList.size()==0. There is not content for all these pmids of size "+pmids.size());
			return null;
		}
		List<Map<String, TermFreqVector>> mapList = new ArrayList<Map<String,TermFreqVector>>();
		for(int i=0; i< termVecList.size() ; i++){
			TermFreqVector[] termVecs = termVecList.get(i);
			Map<String, TermFreqVector> fieldMap = new HashMap<String, TermFreqVector>();
			for(TermFreqVector vec: termVecs){
				fieldMap.put(vec.getField(), vec);
//				logger.info("doc"+i+"\tfield:"+vec.getField());
			}
			mapList.add(fieldMap);
		}
		
		Map<String, Integer> tfMap = mergeTermVectors(mapList, TEXT_FIELD); // use abstract only for now.
		Map<String, Float> idfMap = indexAccess.getIDFForField(tfMap.keySet(), TEXT_FIELD);
		Map<String, Float> tfidfMap = getTFIDFMap(tfMap, idfMap);
		
		return tfidfMap;
	}
	
	public static Map<String, Float> getTFIDFMap(Map<String, Integer> tfMap, Map<String, Float> idfMap){
		Map<String, Float> tfidfMap = new HashMap<String, Float>();
		Set<String> keys = tfMap.keySet();
		for(String key: keys){
			Integer tf = tfMap.get(key);
			Float idf = idfMap.get(key);
			float tfidf = tf*idf;
			tfidfMap.put(key, tfidf);
		}
		return tfidfMap;
	}
	
	public static Map<String, Integer> mergeTermVectors(List<Map<String, TermFreqVector>> mapList, String field){
		List<TermFreqVector> termVectors = new ArrayList<TermFreqVector>();
		for(Map<String, TermFreqVector> map: mapList){
			TermFreqVector vec = map.get(field);
			if(vec !=null){
				termVectors.add(vec);
			}else
				logger.info("Vec is null for field:"+field);
		}
		Map<String, Integer> freqMap = new HashMap<String, Integer>();
		for(TermFreqVector vector: termVectors){
			String[] terms = vector.getTerms();
			int[] freqs = vector.getTermFrequencies();
			for(int i=0; i< terms.length; i++){
				Integer freq = freqMap.get(terms[i]);
				if(freq== null){
					freq = 0;
				}
				freq+= freqs[i];
				freqMap.put(terms[i], freq);
			}
		}
		return freqMap;
	}
	
	public int getEndYear() {
		return endYear;
	}

	public void setEndYear(int year) {
		this.endYear = year;
	}
	

	public int getStartYear() {
		return startYear;
	}

	public void setStartYear(int startYear) {
		this.startYear = startYear;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length !=3){
			System.out.println("--pmid-path --save --year");
			return;
		}
		InterDisipline inter;
		try {
			inter = new InterDisipline(Integer.parseInt(args[2]));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
