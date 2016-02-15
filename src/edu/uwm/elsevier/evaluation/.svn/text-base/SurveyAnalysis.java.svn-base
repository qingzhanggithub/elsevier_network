/**
 * 
 */
package edu.uwm.elsevier.evaluation;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.processing.Filer;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;

import articlesdata.article.ArticleService;
import articlesdata.article.Citation;
import articlesdata.citation.CitationService;

import com.csvreader.CsvReader;

import edu.uwm.elsevier.CitationArticleComparison;
import edu.uwm.elsevier.CitationMerger;
import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.CitationSearcher;
import edu.uwm.elsevier.MappingStatus;
import edu.uwm.elsevier.analysis.Counter;

/**
 * @author qing
 *
 */
public class SurveyAnalysis {
	private static String FILE_ROOT="/home/qzhang/network/";
//	private static String FILE_ROOT="/Users/qing/wind3/network/";
	private CitationNetworkService citationNetworkService;
	private CitationService citationService;
	private CitationMerger citationMerger;
	private static String CITART_MAP_KEY = "CITART_MAP_KEY";
	private static String ELSPM_MAP_KEY = "ELSPM_MAP_KEY";
	private static String CITCIT_MAP_KEY = "CITCIT_MAP_KEY";
	private Logger LOGGER = Logger.getLogger(SurveyAnalysis.class);
	private FileWriter debugWriter;
	private FileWriter reportWriter;
	private CitationService citaitonService;
	private ArticleService articleService;
	private RecallSampleGenerator recallGen;
	
	public SurveyAnalysis() throws ClassNotFoundException, SQLException, IOException{
		citationNetworkService = new CitationNetworkService();
		citationMerger = new CitationMerger("/home/qzhang/elsevier_citation_index2");
		citationService = new CitationService();
		citaitonService = new CitationService();
		articleService = new ArticleService();
		recallGen = new RecallSampleGenerator();
	}
	public SurveyAnalysis(String fileRoot){
		
	}
	
	
	public void initWriters() throws IOException{
		debugWriter = new FileWriter("/home/qzhang/debug.txt");
		reportWriter = new FileWriter("/home/qzhang/survey_report.tsv");
	}
	
	public void closeWriters() throws IOException{
		if(debugWriter != null)
			debugWriter.close();
		if(reportWriter!=null)
			reportWriter.close();
	}
	
	public void getAgreement(String anno1, String anno2) throws IOException{
		List<String> keys1 = loadIdStrings(anno1);
		List<String> keys2 = loadIdStrings(anno2);
		Set<String> keySet1 = new HashSet<String>(keys1);
		Set<String> keySet2 = new HashSet<String>(keys2);
		Set<String> citartOverlap = new HashSet<String>();
		Set<String> elspmOverlap = new HashSet<String>();
		Set<String> citcitOverlap = new HashSet<String>();
		
		for(String key1: keySet1){
			if(keySet2.contains(key1)){
				if(key1.indexOf(RecallSampleGenerator.CITATION_ARTICLE_ID)!= -1){
					citartOverlap.add(key1);
				}else if(key1.indexOf(RecallSampleGenerator.ELSEVIER_MEDLINE_ID)!= -1){
					elspmOverlap.add(key1);
				}else if(key1.indexOf(RecallSampleGenerator.CIATION_CITATION_ID)!= -1){
					citcitOverlap.add(key1);
				}
			}
		}
		HashMap<String, Set<String>> checkMap1 = parseResultLongURL(FILE_ROOT+anno1+"url.txt");
		HashMap<String, Set<String>> checkMap2 = parseResultLongURL(FILE_ROOT+anno2+"url.txt");
		float citArtAgree = calAgreement(citartOverlap, checkMap1, checkMap2);
		LOGGER.info("citArtAgree="+citArtAgree);
		LOGGER.info("----");
		float elspmAgree = calAgreement(elspmOverlap, checkMap1, checkMap2);
		LOGGER.info("elspmAgree="+elspmAgree);
		LOGGER.info("---");
		float citcitAgree= calAgreement(citcitOverlap, checkMap1, checkMap2);
		LOGGER.info("citcitAgre="+citcitAgree);
	}
	
	public float calAgreement(Set<String> overlap, 
			HashMap<String, Set<String>> checkMap1,
			HashMap<String, Set<String>> checkMap2){
		int yesyes = 0;
		int yesno= 0;
		int noyes = 0;
		int nono = 0;
		for(String key: overlap){
			Set<String> check1 = checkMap1.get(key);
			Set<String> check2 = checkMap2.get(key);
			if(check1 ==null && check2== null){
				nono++;
			}else if(check1== null && check2 !=null){
				noyes+= check2.size();
			}else if(check1 != null && check2 == null){
				yesno+= check1.size();
			}else{
				Set<String> inter = getOverlap(check1, check2);
				yesyes+= inter.size();
				yesno+=check1.size() - inter.size();
				noyes += check2.size()-inter.size();
			}
		}
		int total = yesyes+ yesno+ noyes+ nono;
		
		float p11= yesyes*1.0f /total;
		if (p11 ==1)
			return 1.0f;
		float p12 = noyes * 1.0f /total;
		float p21 = yesno * 1.0f / total;
		float p22 = nono *1.0f /total;
		LOGGER.info("p11="+p11+"\tp12="+p12+"\tp21="+p21+"\tp22="+p22);
		float p0 = p11 + p22;
		float pe = (p11+p21)* (p11+p12) + (p21+p22)* (p12+p22);
		float kappa = (p0- pe)* 1.0f / (1-pe);
		System.out.println("p0="+p0+"\tpe="+pe);
		return kappa;
		
	}
	
	private Set<String> getOverlap(Set<String> set1, Set<String> set2){
		Set<String> overlap = new HashSet<String>();
		for(String s: set1){
			if(set2.contains(s))
				overlap.add(s);
		}
		return overlap;
	}
	
	public void doEvaluation() throws NumberFormatException, SQLException, IOException, ParseException{
		LOGGER.info("task start.");
		String[] names = new String[]{"1", "2", "3", "5","6", "7", "8"};
		ScoreEntity overall = new ScoreEntity();
		reportWriter.append(overall.getHeader()).append('\n');
		for(String name: names){
			ScoreEntity score = new ScoreEntity();
			evaluate(name, FILE_ROOT+name+"url.txt", score);
			overall.citartSys +=score.citartSys;
			overall.citartGold +=score.citartGold;
			overall.elspmSys += score.elspmSys;
			overall.elspmGold += score.elspmGold;
			overall.citcitSys += score.citcitSys;
			overall.citcitGold += score.citcitGold;
			System.out.println(score.toString());
			reportWriter.append(name).append('\t');
			reportWriter.append(score.toString()).append('\n');
		}
		overall.getCitartPrec();
		overall.getElspmPrec();
		overall.getCitcitRecall();
		LOGGER.info("---\nOverall:\n"+overall.toString());
		reportWriter.append("Overall\t").append(overall.toString()).append('\n');
	}
	
	public void doEvaluationForPrecAndRecall() throws NumberFormatException, SQLException, IOException, ParseException{
		LOGGER.info("task start.");
		String[] names = new String[]{"1", "2", "3", "5","6", "7", "8"};
		ScoreEntity overall = new ScoreEntity();
		for(String name: names){
			ScoreEntity score = new ScoreEntity();
			evaluate(name, FILE_ROOT+name+"url.txt", score);
			LOGGER.info("citart: "+score.citartCounter.toString());
			LOGGER.info("elspm: "+score.elspmCounter.toString());
			LOGGER.info("citcit: "+score.citcitCounter.toString());
			reportWriter.append("citart: "+score.citartCounter.toString()).append("\n");
			reportWriter.append("elspm: "+score.elspmCounter.toString()).append("\n");
			reportWriter.append("citcit: "+score.citcitCounter.toString()).append("\n");
			overall.citartCounter.addAll(score.citartCounter);
			overall.elspmCounter.addAll(score.elspmCounter);
			overall.citcitCounter.addAll(score.citcitCounter);
		}
		LOGGER.info("Overall\ncitart: prec="+overall.citartCounter.getPrec()+"\t"+overall.citartCounter.getRecall());
		LOGGER.info("Overall\nelspm: prec="+overall.elspmCounter.getPrec()+"\t"+overall.elspmCounter.getRecall());
		LOGGER.info("Overall\ncitcit: prec="+overall.citcitCounter.getPrec()+"\t"+overall.citcitCounter.getRecall());
		reportWriter.append("overall-citart: "+overall.citartCounter.toString()).append("\n");
		reportWriter.append("overall-elspm: "+overall.elspmCounter.toString()).append("\n");
		reportWriter.append("overall-citcit: "+overall.citcitCounter.toString()).append("\n");
		LOGGER.info("Task done.");
	}
	
	public void evaluate(String keyPath, String resultPath, ScoreEntity score) throws NumberFormatException, SQLException, IOException, ParseException{
		LOGGER.info(keyPath);
		debugWriter.append(keyPath).append('\n');
		HashMap<String, Set<String>> sysResult = getSystemResult(keyPath);
		HashMap<String, Set<String>> checkMap = parseResultLongURL(resultPath);
		Set<String> keys = sysResult.keySet();
		for(String key: keys){
			debugWriter.append("=== checking entity:"+key+" ===");
			Set<String> sys = sysResult.get(key);
			debugWriter.append("[SYS]\n"+sys.toString()).append("\n");
			Set<String> gold = checkMap.get(key);
			
			if(gold == null){
				LOGGER.info("gold is null for "+keyPath+", and sys.size="+sys.size());
				debugWriter.append("gold is null for "+key+", and sys.size="+sys.size()).append('\n');
				continue;
			}
			debugWriter.append("[GOLD]\n"+gold.toString()).append("\n");
			debugWriter.append("sysSize:").append(String.valueOf(sys.size())).append('\t').append("goldSys:").append(String.valueOf(gold.size())).append('\n');
			if(key.startsWith(RecallSampleGenerator.CITATION_ARTICLE_ID)){
				score.citartGold+= gold.size();
				for(String g: gold){
					if(sys.contains(g)){
						score.citartCounter.addToTp(1);
					}
					else{
						score.citartCounter.addToFn(1);
						debugWriter.append("CITART FN GOLD:"+g+"\n");
					}
				}
				for(String s: sys){
					if(!gold.contains(s)){
						score.citartCounter.addToFp(1);
						debugWriter.append("CITART FP GOLD:"+s+"\n");
					}
				}
				int tn = sys.size() - score.citartCounter.getTp()-score.citartCounter.getFn()-score.citartCounter.getFp();
				score.citartCounter.setTn(tn);
			}else if(key.startsWith(RecallSampleGenerator.ELSEVIER_MEDLINE_ID)){
				score.elspmGold+= gold.size();
				for(String g: gold){
					if(sys.contains(g))
						score.elspmCounter.addToTp(1);
					else{
						score.elspmCounter.addToFn(1);
						debugWriter.append("ELSPM FN GOLD:"+g+"\n");
					}
				}
				for(String s: sys){
					if(!gold.contains(s)){
						score.elspmCounter.addToFp(1);
						debugWriter.append("ELSPM FP SYS:"+s+"\n");
					}
				}
				int tn = sys.size() - score.elspmCounter.getTp()-score.elspmCounter.getFn()-score.elspmCounter.getFp();
				score.elspmCounter.setTn(tn);
			}else if(key.startsWith(RecallSampleGenerator.CIATION_CITATION_ID)){
				gold = expendCitcitGold(gold);
				score.citcitGold+= gold.size();
				for(String g: gold){
					if(sys.contains(g))
						score.citcitCounter.addToTp(1);
					else{
						score.citcitCounter.addToFn(1);
						debugWriter.append("CITCIT FN - gold:"+g+"\n");
					}
				}
				for(String s: sys){
					if(!gold.contains(s)){
						score.citcitCounter.addToFp(1);
						debugWriter.append("CITCIT FP - sys:"+s+"\n");
					}
				}
				int tn = sys.size() - score.citcitCounter.getTp()-score.citcitCounter.getFn()-score.citcitCounter.getFp();
				score.citcitCounter.setTn(tn);
			}
		}
//		score.getCitartPrec();
//		score.getElspmPrec();
//		score.getCitcitRecall();
	}
	
	public HashMap<String, Set<String>>  parseResultLongURL(String resultPath) throws IOException{
		FileInputStream fstream = new FileInputStream(resultPath);
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  HashMap<String, Set<String>> checkMap = new HashMap<String, Set<String>>();
		  while ((strLine = br.readLine()) != null)   {
			  String[] pairs = strLine.split("&");
			  for(String p: pairs){
				 String[] fields = p.split("=");
				 addPair(fields, checkMap);
			  }
		  }
		  return checkMap;
	}
	
	public List<Long> getCorrectSys(long citationId) throws SQLException, IOException, ParseException{
		Citation citation = citaitonService.getCitationByCitationId(citationId);
		List<Citation> citations = recallGen.getRecallFromCitationIndex(citation, true, true);
		List<Long> ids = new ArrayList<Long>();
		List<Citation> result = new ArrayList<Citation>();
		Set<Long> idResult = new HashSet<Long>();
		for(Citation c: citations){
			long root = citationNetworkService.getRootByCitationId(c.getCitationId());
//			LOGGER.info("root="+root);
			if(root == -1 || (ids.contains(root) || ids.contains(c.getCitationId())))
				continue;
			ids.add(root);
			ids.add(c.getCitationId());
			if(citation.getTitle()==null || c.getTitle()==null)
				continue;
			if(c.getCitationId() != citationId ){
				MappingStatus status = new MappingStatus();
				CitationArticleComparison.isCitationAndCitationEqual(c, citation, status);
				if(status.isEqual()){
					result.add(c);
					idResult.add(c.getCitationId());
				}
			}
			Citation rootCitation = citationService.getCitationByCitationId(root);
			if(rootCitation.getCitationId()!= citationId){
				MappingStatus status = new MappingStatus();
				CitationArticleComparison.isCitationAndCitationEqual(rootCitation, citation, status);
				if(status.isEqual()){
					result.add(rootCitation);
					idResult.add(root);
				}
			}
		}
		return new ArrayList<Long>(idResult);
	}
	
	public Set<String>  expendCitcitGold(Set<String> goldIds) throws SQLException{
		Set<String> expended = new HashSet<String>();
		for(String goldId: goldIds){
			Set<Long> ids = citationNetworkService.getAllEquavilentCitationsForCitation(Long.parseLong(goldId));
			for(long id: ids){
				expended.add(String.valueOf(id));
			}
		}
		return expended;
	}
	
//	public List<Integer> getSysForCitCit(String key){
//		long citationId = Long.parseLong(key);
//	}
	
	public static List<String> loadIdStrings(String user){
		List<String> ids = new ArrayList<String>();
		try {
			CsvReader csv = new CsvReader(FILE_ROOT+user+"key.txt");
			while(csv.readRecord()){
				ids.add(csv.get(0));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ids;
	}
	public HashMap<String, Set<String>>  getSystemResult(String keyPath) throws NumberFormatException, SQLException, IOException, ParseException{
		List<String> ids = loadIdStrings(keyPath);
		HashMap<String, Set<String>> resultMap = new HashMap<String, Set<String>>();
		for(String id : ids){
			String idStr = getIdStr(id);
			LOGGER.info("Getting sys for id:"+idStr+"("+id+")");
			if(idStr == null)
				continue;
			if(id.startsWith(RecallSampleGenerator.CITATION_ARTICLE_ID)){
				int articleId = citationNetworkService.getArticleIdByInciteCitationId(Long.parseLong(idStr));
				if(articleId !=-1){
				String articleIdStr = String.valueOf(articleId);
				addPair(new String[]{id, articleIdStr}, resultMap);
				}
				
			}else if(id.startsWith(RecallSampleGenerator.ELSEVIER_MEDLINE_ID)){
				long pmid = citationNetworkService.getPMIDByArticleId(Integer.parseInt(idStr));
				if(pmid !=-1){
				String pmidStr = String.valueOf(pmid);
				addPair(new String[]{id, pmidStr}, resultMap);
				}
			}else if(id.startsWith(RecallSampleGenerator.CIATION_CITATION_ID)){
				LOGGER.info("Getting equ citations ...");
				Set<Long> equsCorr = new HashSet<Long>(getCorrectSys(Long.parseLong(idStr)));
				LOGGER.info("equsCorr:"+equsCorr.size());
//				Set<Long> equs = citationNetworkService.getAllEquavilentCitationsForCitation(Long.parseLong(idStr));
//				LOGGER.info("equs:"+equs.size());
//				debugWriter.append("equsCorr="+equsCorr.size()).append('\t').append("org="+equs.size());
				for(long eq: equsCorr){
					String eqIdStr = String.valueOf(eq);
					addPair(new String[]{id, eqIdStr}, resultMap);
				}
				LOGGER.info("Got equ citations ...");
			}
		}
		return resultMap;
	}
	
	private String getIdStr(String s){
		Pattern p = Pattern.compile("[0-9]+");
		Matcher m = p.matcher(s);
		if(m.find()){
			return m.group();
		}
		return null;
	}
	
	private void addPair(String[] pair, HashMap<String, Set<String>> map){
		Set<String> list = map.get(pair[0]);
		if(list == null){
			list = new HashSet<String>();
		}
		list.add(pair[1]);
		map.put(pair[0], list);
	}
	
	class ScoreEntity{
		int citartSys = 0;
		int citartGold = 0;
		Counter citartCounter = new  Counter();
		
		int elspmSys = 0;
		int elspmGold = 0;
		Counter elspmCounter = new Counter();
		
		int citcitSys = 0;
		int citcitGold = 0;
		Counter citcitCounter = new Counter();
		
		float citartPrec =0f;
		float elspmPrec =0f;
		float citcitRecall = 0f;
		
		
		public float getCitartPrec(){
			citartPrec = citartSys*1.0f/citartGold;
			return citartPrec;
		}
		
		public float getElspmPrec(){
			elspmPrec = elspmSys*1.0f/elspmGold;
			return elspmPrec;
		}
		
		public float getCitcitRecall(){
			citcitRecall = citcitSys*1.0f/citcitGold;
			return citcitRecall;
		}
		
		public String toString(){
			StringBuffer sb = new StringBuffer();
//			sb.append("citartSys:").append(citartSys).append('\t').append("citartGold:").append(citartGold).append('\t').append("prec:").append(citartPrec).append('\n');
//			sb.append("elspmSys:").append(elspmSys).append('\t').append("elspmGold:").append(elspmGold).append('\t').append("prec:").append(elspmPrec).append('\n');
//			sb.append("citcitSys:").append(citcitSys).append('\t').append("citcitGold:").append(citcitGold).append('\t').append("recall:").append(citcitRecall).append('\n');
			sb.append(citartSys).append('/').append(citartGold).append('\t').append(citartPrec).append('\t');
			sb.append(elspmSys).append('/').append(elspmGold).append('\t').append(elspmPrec).append('\t');
			sb.append(citcitSys).append('/').append(citcitGold).append('\t').append(citcitRecall);
			
			return sb.toString();
		}
		
		public  String getHeader(){
			StringBuffer sb =new StringBuffer();
			sb.append("Annotator").append('\t');
			sb.append("C-A sys/gold").append('\t');
			sb.append("C-A prec").append('\t');
			sb.append("E-M sys/gold").append('\t');
			sb.append("E-M prec").append('\t');
			sb.append("C-C sys/gold").append('\t');
			sb.append("C-C recall");
			return sb.toString();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			SurveyAnalysis analysis = new SurveyAnalysis();
			analysis.initWriters();
//			analysis.doEvaluation();
			analysis.doEvaluationForPrecAndRecall();
			analysis.closeWriters();
//			 analysis.getAgreement("1", "2");
//			 analysis.getAgreement("2", "3");
//			 analysis.getAgreement("5", "6");
//			 analysis.getAgreement("6", "7");
//			 analysis.getAgreement("7", "8");
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
