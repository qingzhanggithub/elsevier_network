/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import com.csvreader.CsvReader;

import edu.uwm.elsevier.NetworkBuilderLogger;
import edu.uwm.elsevier.namedisambiguation.VectorBuilder.SimilarityInfo;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class VectorBuilder {
	
	private static String SELECT_INCITE_ARTICLE = "select citation_qing.article_id from citation_qing, cnetworkv4" +
			" where cnetworkv4.article_id = ? and cnetworkv4.citation_id = citation_qing.citation_id";
	private PreparedStatement prepSelectIncite;
	private ArticlesDataDBConnection databaseConnection;
	private MedlineSearcherForDSB searcher;
	public static String FIRST_NAME_KEY = "first_name";
	public static String MIDDLE_NAME_KEY = "middle_name";
	private static String RESULT_SAVE = "/home/qzhang/namedsb_result.txt";
	private static FileWriter writer ;
	public static String NAME_NORMALIZE_ALL = "NORM-ALL";
	public static String NAME_NORMALIZE_EXTEND = "NORM-EXT";
//	private static Logger LOGGER = NetworkBuilderLogger.getLogger("AuthorDSB");
	
	public VectorBuilder() throws ClassNotFoundException, SQLException, IOException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
		prepSelectIncite = databaseConnection.getConnection().prepareStatement(SELECT_INCITE_ARTICLE);
		searcher = new MedlineSearcherForDSB();
		writer = new FileWriter(RESULT_SAVE);
	}

	public void getVector(String path) throws IOException, SQLException, ParseException{
		CsvReader csv = new CsvReader(path, '\t');
		List<AuthorVecInfo> listToCompare =null;
		AuthorVecInfo prev = null;
		AuthorVecInfo current = null;
		int index  =0;
		while(csv.readRecord()){
			System.out.println("in record "+index);
			if(prev== null && current==null// first row
					|| prev!= null && current!=null && !current.isEqual(prev)){	// new name
				if(listToCompare!=null){
					processWithinSameLastNameSimple(listToCompare);
					listToCompare.clear();
				}else
					listToCompare = new ArrayList<AuthorVecInfo>();
			}
			prev = current;
			int authorId = Integer.parseInt(csv.get(0));
			AuthorVecInfo authorVec = new AuthorVecInfo(authorId);
			authorVec.setLastName(csv.get(1));
			if(csv.get(2)!=null){	// separate first name from middle name
				HashMap<String, String> map = parseGivenName(csv.get(2));
				authorVec.setFirstName(map.get(FIRST_NAME_KEY));
				authorVec.setMiddleName(map.get(MIDDLE_NAME_KEY));
			}
			authorVec.setArticleId(Integer.parseInt(csv.get(3)));
			authorVec.setPmid(Integer.parseInt(csv.get(4)));
			authorVec.setInciteArticleIds(getIncitesFromDB(authorVec.getArticleId()));
//			float[] tfidfs = getTermVector(authorVec.getPmid());
//			if(tfidfs !=null)
//				authorVec.setTfidfs(tfidfs);
			listToCompare.add(authorVec);
			current =authorVec;
			index ++;
		}
		closeAllStuff();
		System.out.println("Finished.");
	}
	
	public void processWithinSameLastNameSimple(List<AuthorVecInfo> authors){
		for(AuthorVecInfo authorVec: authors){
			normalizeAuthorVec(authorVec);
		}
		List<SimilarityInfo> simScores = computeSimilarity(authors);
	}
	
	private boolean isTotallyDifferent(String src, String dest){
		boolean isEqual = false;
		if(src.equals(dest))// all initial / all full / all empty
			isEqual = true;
		else if(src.indexOf(NAME_NORMALIZE_ALL)!=-1 || // one is empty
				dest.indexOf(NAME_NORMALIZE_ALL)!=-1)
			isEqual = true;
		else if(src.indexOf(NAME_NORMALIZE_EXTEND)!=-1 && dest.startsWith(String.valueOf(src.charAt(0)))||
				dest.indexOf(NAME_NORMALIZE_EXTEND)!=-1 && src.startsWith(String.valueOf(dest.charAt(0))))
			isEqual = true;
		return isEqual;
	}
	
	private void normalizeAuthorVec(AuthorVecInfo authorVec){
		if(authorVec.getFirstName()== null){
			authorVec.setFirstName(NAME_NORMALIZE_ALL);
		}else if(authorVec.getNameType(authorVec.getFirstName()).equals("0")){
			String normalized = authorVec.getFirstName()+"-"+NAME_NORMALIZE_EXTEND;
			authorVec.setFirstName(normalized);
		}
		if(authorVec.getMiddleName()== null){
			authorVec.setMiddleName(NAME_NORMALIZE_ALL);
		}else if(authorVec.getNameType(authorVec.getMiddleName()).equals("0")){
			String normalized = authorVec.getMiddleName()+"-"+NAME_NORMALIZE_EXTEND;
			authorVec.setMiddleName(normalized);
		}
	}
	
	public void printSimScore(List<SimilarityInfo> simScores) throws IOException{
		for(SimilarityInfo sim: simScores){
			writer.append(sim.toString()).append('\n');
		}
	}
	
	public void printSimScoreMap(HashMap<String, List<SimilarityInfo>> scoreMap) throws IOException{
		Collection<List<SimilarityInfo>> scoreMapValues = scoreMap.values();
		for(List<SimilarityInfo> simScores: scoreMapValues){
			printSimScore(simScores);
		}
	}
	
	public void closeAllStuff() throws IOException{
		if(searcher !=null)
			searcher.close();
		if(writer !=null)
			writer.close();
	}
	
	public static boolean isMiddleNameEqual(String srcMiddle, String targetMiddle){
		if(srcMiddle != null && targetMiddle != null && (srcMiddle.indexOf(targetMiddle)!= -1 || targetMiddle.indexOf(srcMiddle)!=-1))
			return true; // no middle name.
		else if (srcMiddle == null || targetMiddle == null)
			return true;	// cannot compare, return true;
		else
			return false;
	}
	
	public List<SimilarityInfo> computeSimilarity(List<AuthorVecInfo> list){
		int size = list.size();
		List<SimilarityInfo> similarityList = new ArrayList<VectorBuilder.SimilarityInfo>();
		for(int i=0; i<size; i++){
			AuthorVecInfo src = list.get(i);
			for(int j=i+1; j<size; j++){
				AuthorVecInfo target = list.get(j);
				if(isTotallyDifferent(src.getFirstName(), target.getFirstName()) || 
						isTotallyDifferent(src.getMiddleName(), target.getMiddleName()))
					continue; // don't need to compare. they are totally different
				SimilarityInfo simInfo = new SimilarityInfo(src.getAuthorId(), target.getAuthorId());
//				simInfo.tfidfSimilarity = getSimilarityByTFIDF(src, target);
				simInfo.inciteSimilarity = getSimilarityByIncite(src, target);
				similarityList.add(simInfo);
			}
		}
		return similarityList;
	}
	
	public List<SimilarityInfo> computeSimilarityBetweenLists(List<AuthorVecInfo> srcList, List<AuthorVecInfo> targetList){
		List<SimilarityInfo> similarityList = new ArrayList<VectorBuilder.SimilarityInfo>();
		for(AuthorVecInfo src: srcList){
			for(AuthorVecInfo target: targetList){
				SimilarityInfo simInfo = new SimilarityInfo(src.getAuthorId(), target.getAuthorId());
//				simInfo.tfidfSimilarity = getSimilarityByTFIDF(src, target);
				simInfo.inciteSimilarity = getSimilarityByIncite(src, target);
				similarityList.add(simInfo);
			}
		}
		return similarityList;
	}
	
	public float getSimilarityByIncite(AuthorVecInfo src, AuthorVecInfo target){
		List<Integer> srcInsite = src.getInciteArticleIds();
		List<Integer> targetInsite = target.getInciteArticleIds();
		if(srcInsite==null || srcInsite.size()==0 || targetInsite== null|| targetInsite.size()==0)
			return 0;
		HashSet<Integer> srcset = new HashSet<Integer>(srcInsite);
		HashSet<Integer> tarset = new HashSet<Integer>(targetInsite);
		srcset.retainAll(tarset);
		return srcset.size()*1.0f/(float)(Math.sqrt((double)srcInsite.size())* Math.sqrt((double)targetInsite.size()));
	}
	
	public float getSimilarityByTFIDF(AuthorVecInfo src, AuthorVecInfo target){
		float[] srcTfidf = src.getTfidfs();
		float[] targetTfidf = target.getTfidfs();
		int length = srcTfidf.length;
		float sum = 0;
		float srcSqsum =0;
		float targetSqsum = 0;
		for(int i=0; i<length; i++){
			sum += srcTfidf[i] * targetTfidf[i];
			srcSqsum += srcTfidf[i] * srcTfidf[i];
			targetSqsum += targetTfidf[i] * targetTfidf[i];
		}
		return sum/(float)(Math.sqrt((double)srcSqsum) * Math.sqrt((double)targetSqsum));
	}
	
	public static void addToMap(String key, AuthorVecInfo value, HashMap<String, List<AuthorVecInfo>> map){
		List<AuthorVecInfo> list = map.get(key);
		if(list== null){
			list = new ArrayList<AuthorVecInfo>();
		}
		list.add(value);
		map.put(key, list);
	}
	
	public void getOutciteVector(){
		
	}
	
	public static HashMap<String,String> parseGivenName(String givenName){
		String[] names = givenName.split(".\\s");
		HashMap<String,String> nameMap = new HashMap<String, String>();
		String firstName = null;
		String middleName = null;
		for(String name: names){
			if(name.trim().length() !=0 ){
				if(firstName == null)
					firstName = name.trim();
				else if(middleName == null)
					middleName = name.trim();
				else
					break;
			}
		}
		nameMap.put(FIRST_NAME_KEY, firstName);
		nameMap.put(MIDDLE_NAME_KEY, middleName);
		return nameMap;
	}
	
	public List<Integer> getIncitesFromDB(int articleId) throws SQLException{
		prepSelectIncite.setInt(1, articleId);
		ResultSet rs = prepSelectIncite.executeQuery();
		List<Integer> articleIds = new ArrayList<Integer>();
		while(rs.next()){
			articleIds.add(rs.getInt(1));
		}
		rs.close();
		return articleIds;
	}
	
	public float[] getTermVector(int pmid) throws ParseException, IOException{
		float[] tfidfs = searcher.getTfidfByPMID(pmid);
		return tfidfs;
	}
	
	class SimilarityInfo{
		int srcAuthorId;
		int targetAuthorId;
		float tfidfSimilarity = -1;
		float inciteSimilarity = -1;
		float outciteSimilarity = -1;
		public SimilarityInfo(int srcAuthorId, int targetAuthorId){
			this.srcAuthorId = srcAuthorId;
			this.targetAuthorId = targetAuthorId;
		}
		public String toString(){
			StringBuffer sb = new StringBuffer();
			sb.append(srcAuthorId).append('\t');
			sb.append(targetAuthorId).append('\t');
			sb.append(inciteSimilarity).append('\t');
			sb.append(outciteSimilarity).append('\t');
			sb.append(tfidfSimilarity);
			return sb.toString();
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String path = "/Users/qing/wind3/author_names_test.txt";
		try {
			VectorBuilder vb = new VectorBuilder();
			vb.getVector(args[0]);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
