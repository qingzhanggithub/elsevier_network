/**
 * 
 */
package edu.uwm.elsevier.evaluation;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import pmidmapper.MedlineSearcher;
import pmidmapper.PMArticle;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.article.Citation;
import articlesdata.citation.CitationService;

import edu.uwm.elsevier.CitationArticleComparison;
import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.CitationSearcher;
import edu.uwm.elsevier.ElsevierArticle;
import edu.uwm.elsevier.ElsevierArticleMetaDataSearcher;

/**
 * @author qing
 *
 */
public class RecallSampleGenerator {

	
	private String pubmedIndex = "/home/data_user/pubmed_index";
	private String articleIndex = "/home/qzhang/elsevier_index";
	private String citationIndex = "/home/qzhang/elsevier_citation_index2"; 
	private ElsevierArticleMetaDataSearcher elsevierMetaSearcher;
	private CitationSearcher citationSearcher ;
	private MedlineSearcher medlineSearcher;
	private CitationService citationService;
	private ArticleService articleService;
	private AuthorService authorService;
	private CitationNetworkService citationNetworkService;
	public static String CITATION_ARTICLE_ID = "CITART";
	public static String CIATION_CITATION_ID = "CITCIT";
	public static String ELSEVIER_MEDLINE_ID = "ELSEPM";
	private  int sampleSize = 5;
	private int maxCitation = 9000;
	private int maxArticle = 9000;
	private static Logger LOGGER = SamplingLogger.getLogger("RecallSampleGenerator");
	
	public RecallSampleGenerator() throws IOException, ClassNotFoundException, SQLException{
		elsevierMetaSearcher = new ElsevierArticleMetaDataSearcher(articleIndex);
		citationSearcher = new CitationSearcher(citationIndex);
		medlineSearcher = new MedlineSearcher(pubmedIndex);
		citationService = new CitationService();
		articleService = new ArticleService();
		authorService = new AuthorService();
		citationNetworkService = new CitationNetworkService();
		maxCitation = citationNetworkService.getMergingTableCitationCount();
		maxArticle = citationNetworkService.getElsMLTableCount();
	}
	
	public void getRecallSampleEvaluationTable(String save) throws SQLException, IOException, ParseException, ClassNotFoundException{
		LOGGER.info("Start generating recall table...");
		StringBuffer page = new StringBuffer();
		HashMap<Citation, List<ElsevierArticle>> citationArticleMap = getArticlesByCitations();
		String citationArticleTable = getHTMLForCitationArticleCandidatesRow(citationArticleMap, null);
		HashMap<Citation, List<Citation>> citationCitationMap = getCitationsByCitations();
		String citationCitationTable = getHTMLForCitationCitationCandidatesRow(citationCitationMap, null);
		HashMap<ElsevierArticle, List<PMArticle>> elsPMMap = getPMArticlesByElsevier();
		String elsPMTable = getHTMLForElsevierMedlineCandidateRow(elsPMMap, null);
		
		page.append("<table border=1 >");
		page.append("<tr><td colspan=4>CITATION-ARTICLE MAPPING</td></tr>");
		page.append(citationArticleTable);
		page.append("<tr><td colspan=4>CITATION-CITATION MAPPING</td></tr>");
		page.append(citationCitationTable);
		page.append("<tr><td colspan=4>ELSEVIER-MEDLINE MAPPING</td></tr>");
		page.append(elsPMTable);
		page.append("</table>");
		FileWriter writer = new FileWriter(save);
		writer.append(page.toString());
		writer.close();
		LOGGER.info("Finished.");
	}
	
	public List<ElsevierArticle> getRecallFromArticleIndex(Citation citation, boolean useTitle, boolean useAuthor) throws IOException{
		LOGGER.info("Searching article index..");
		BooleanQuery query = null ;
		if(useTitle && useAuthor){
			query = ElsevierArticleMetaDataSearcher.getQuery(citation.getTitle(), citation.getAuthors());
		}else if(useTitle){
			query = ElsevierArticleMetaDataSearcher.getQueryByTitle(citation.getTitle());
		}else if(useAuthor){
			query = ElsevierArticleMetaDataSearcher.getQueryByCitationAuthors(citation.getAuthors());
		}
		List<ElsevierArticle> articles = elsevierMetaSearcher.getElsevierArticlesByQuery(query);
		return articles;
	}
	
	public List<Citation> getRecallFromCitationIndex(Citation citation, boolean useTitle, boolean useAuthor) throws IOException, ParseException{
		LOGGER.info("Searching citation index...");
		BooleanQuery query = null;
		if(useTitle && useAuthor){
			query = ElsevierArticleMetaDataSearcher.getQuery(citation.getTitle(), citation.getAuthors());
		}else if(useTitle){
			query = ElsevierArticleMetaDataSearcher.getQueryByTitle(citation.getTitle());
		}else if(useAuthor){
			query = ElsevierArticleMetaDataSearcher.getQueryByCitationAuthors(citation.getAuthors());
		}
		List<Citation> citations = citationSearcher.getCitationsByQuery(query);
		return citations;
	}
	
	public List<PMArticle> getRecallFromMEDLINEIndex(ElsevierArticle article, boolean useTitle, boolean useAuthor) throws IOException{
		LOGGER.info("Searching medline ...");
		BooleanQuery query = null;
		if(useTitle && useAuthor){
			query = ElsevierArticleMetaDataSearcher.getQueryFromAritcle(article.getTitle(), article.getAuthors());
		}else if(useTitle){
			query = ElsevierArticleMetaDataSearcher.getQueryByTitle(article.getTitle());
		}else if(useAuthor){
			query = ElsevierArticleMetaDataSearcher.getQueryByArticleAuthors(article.getAuthors());
		}
		List<PMArticle> articles = medlineSearcher.getPMArticlesByQuery(query);
		return articles;
	}
	
	public HashMap<ElsevierArticle, List<PMArticle>> getPMArticlesByElsevier() throws SQLException, IOException, ClassNotFoundException{
		LOGGER.info("Getting PM by Els ...");
		List<Integer> articleIds = TableSampler.sampleArticleTable(sampleSize*2, maxArticle);
		HashMap<ElsevierArticle, List<PMArticle>> samples = new HashMap<ElsevierArticle, List<PMArticle>>();
		int index =0;
		for(int articleId: articleIds){
			Article article = articleService.getArticleByArticleId(articleId);
			List<Author> authors = authorService.getAuthorListByArticleId(articleId);
			ElsevierArticle els = new ElsevierArticle(article);
			els.setAuthors(authors);
			List<PMArticle> pmsByTitle = getRecallFromMEDLINEIndex(els, true, false);
			List<PMArticle> pmsByAuthor = getRecallFromMEDLINEIndex(els, false, true);
			List<PMArticle> result = new ArrayList<PMArticle>();
			for(PMArticle a: pmsByTitle){
				if(FuzzyComparison.isTitleFuzzyEqual(els.getTitle(), a.getTitle(), 0.6f))
					result.add(a);
			}
			for(PMArticle a: pmsByAuthor){
				if(CitationArticleComparison.isArticleArticleAuthorListEqual(els.getAuthors(), a.getAuthorList()))
					result.add(a);
			}
			if(result.size() >0){
				index++;
				samples.put(els, result);
				LOGGER.info(index+" sampled.");
				if(samples.size() ==sampleSize)
					break;
			}
		}
		return samples;
	}
	
	
	
	public HashMap<Citation, List<ElsevierArticle>> getArticlesByCitations() throws SQLException, IOException, ClassNotFoundException{
		LOGGER.info("Getting article by citation ...");
		HashMap<Citation, List<ElsevierArticle>> samples = new HashMap<Citation, List<ElsevierArticle>>();
		HashSet<Long> idSet =new HashSet<Long>();
		HashSet<Integer> articleSet = new HashSet<Integer>();
		int index =0;
		while(samples.size() < sampleSize){
			List<Long> citationIds =TableSampler.sampleCitationTable(sampleSize, maxCitation);
			for(long citationId: citationIds){
				if(idSet.contains(citationId))
					continue;// citation has been processed
				idSet.add(citationId);
				Citation citation = citationService.getCitationByCitationId(citationId);
				List<ElsevierArticle> articlesByTitle = getRecallFromArticleIndex(citation, true, false);
				List<ElsevierArticle> articlesByAuthor = getRecallFromArticleIndex(citation, false, true);
				List<ElsevierArticle> result = new ArrayList<ElsevierArticle>();
				for(ElsevierArticle a: articlesByTitle){
					if(!articleSet.contains(a.getArticleId()) && FuzzyComparison.isTitleFuzzyEqual(citation.getTitle(), a.getTitle(), 0.6f)){
						result.add(a);
						articleSet.add(a.getArticleId());
					}
				}
				for(ElsevierArticle a: articlesByAuthor){
					if(!articleSet.contains(a.getArticleId()) && CitationArticleComparison.isCitationArticleAuthorListEqual(citation.getAuthors(), a.getAuthors()))
						result.add(a);
						articleSet.add(a.getArticleId());
				}
				if(result.size() >0){
					index++;
					samples.put(citation, result);
					LOGGER.info(index+" sampled.");
					if(samples.size()== sampleSize)
						break;
				}
			}
		}
		return samples;
	}
	
	public String getHTMLForCitationArticleCandidatesRow(HashMap<Citation, List<ElsevierArticle>> map, Set<Citation> selectedKeys){
		Set<Citation> keys = null;
		if(selectedKeys == null){
			keys = map.keySet();
		}else
			keys = selectedKeys;
		int size = keys.size();
		String colorRowOdd = "<tr style=\"background-color:#FFFFFF\">";
		String colorRowEven = "<tr style=\"background-color:#c2dfff\">";
		StringBuffer sb = new StringBuffer();
		int index = 1;
		for(Citation c: keys){
			String colorRow ;
			if(index%2==1){
				colorRow = colorRowOdd;
			}else
				colorRow = colorRowEven;
			List<ElsevierArticle> articles = map.get(c);
			String citationHTML = Sampler.getCitationHTML(c);
			int articleIndex = 1;
			LOGGER.info("index:"+index);
			for(ElsevierArticle article: articles){
				LOGGER.info("articleIndex:"+articleIndex);
				sb.append(colorRow).append("<td>");
				sb.append(index).append("/").append(size).append(",").append(articleIndex).append("/").append(articles.size()).append("</td>");
				sb.append("<td>");
				sb.append(citationHTML);
				sb.append("</td><td>");
				sb.append(Sampler.getElsevierArticleHTML(article));
				sb.append("</td>");
				sb.append("<td>");
				sb.append("<input type=\"checkbox\" name=").append(CITATION_ARTICLE_ID).append(c.getCitationId()).append(" value=\"").append(article.getArticleId()).append("\" />");
				sb.append("</td></tr>");
				articleIndex++;
			}
			index++;
		}
		return sb.toString();
	}
	
	public String getHTMLForCitationCitationCandidatesRow(HashMap<Citation, List<Citation>> map, Set<Citation> selectedKeys){
		Set<Citation> keys = null;
		if(selectedKeys== null){
			keys = map.keySet();
		}else
			keys = selectedKeys;
		int size =keys.size();
		String colorRowOdd = "<tr style=\"background-color:#FFFFFF\">";
		String colorRowEven = "<tr style=\"background-color:#c2dfff\">";
		StringBuffer sb = new StringBuffer();
		int index =1;
		for(Citation c: keys){
			String colorRow;
			if(index % 2==1){
				colorRow =colorRowOdd;
			}else
				colorRow = colorRowEven;
			List<Citation> targetCitations = map.get(c);
			int  citationIndex =1;
			for(Citation target: targetCitations){
				sb.append(colorRow).append("<td>");
				sb.append(index).append("/").append(size).append(",").append(citationIndex).append("/").append(targetCitations.size()).append("</td>");
				sb.append("</td><td>");
				sb.append(Sampler.getCitationHTML(c));
				sb.append("</td><td>");
				sb.append(Sampler.getCitationHTML(target));
				sb.append("</td>");
				sb.append("<td>");
				sb.append("<input type=\"checkbox\" name=").append(CIATION_CITATION_ID).append(c.getCitationId()).append(" value=\"").append(target.getCitationId()).append("\" />");
				sb.append("</td></tr>");
				citationIndex++;
			}
			index++;
		}
		return sb.toString();
	}
	
	public String getHTMLForElsevierMedlineCandidateRow(HashMap<ElsevierArticle, List<PMArticle>> map, Set<ElsevierArticle> selectedKeys){
		Set<ElsevierArticle> keys = null;
		if(selectedKeys == null){
			keys = map.keySet();
		}else
			keys = selectedKeys;
		int size =keys.size();
		String colorRowOdd = "<tr style=\"background-color:#FFFFFF\">";
		String colorRowEven = "<tr style=\"background-color:#c2dfff\">";
		StringBuffer sb = new StringBuffer();
		int index =1;
		for(ElsevierArticle els: keys){
			String colorRow;
			if(index%2==1){
				colorRow = colorRowOdd;
			}else
				colorRow = colorRowEven;
			List<PMArticle> pms = map.get(els);
			int  pmIndex =1;
			for(PMArticle pm: pms){
				sb.append(colorRow).append("<td>");
				sb.append(index).append("/").append(size).append(",").append(pmIndex).append("/").append(pms.size()).append("</td>");
				sb.append("<td>");
				sb.append(Sampler.getElsevierArticleHTML(els));
				sb.append("</td>");
				sb.append("<td>");
				sb.append(Sampler.getPMArticleHTML(pm));
				sb.append("</td>");
				sb.append("<td>");
				sb.append("<input type=\"checkbox\" name=").append(ELSEVIER_MEDLINE_ID).append(els.getArticleId()).append(" value=\"").append(pm.getPmid()).append("\" />");
				sb.append("</td>");
				sb.append("</tr>");
				pmIndex ++;
			}
			index++;
		}
		return sb.toString();
	}
	
	public HashMap<Citation, List<Citation>> getCitationsByCitations() throws SQLException, IOException, ParseException, ClassNotFoundException{
		LOGGER.info("Getting citation by citation ...");
		List<Long> citationIds = TableSampler.sampleCitationTable(sampleSize*2, maxCitation);
		HashMap<Citation, List<Citation>> samples = new HashMap<Citation, List<Citation>>();
		int index =0;
		for(long citationId: citationIds){
			HashSet<Long> rootSet = new HashSet<Long>();
			Citation citation = citationService.getCitationByCitationId(citationId);
			if(citation.getTitle() == null)
				continue;
			List<Citation> citationsByTitle = getRecallFromCitationIndex(citation, true, false);
			List<Citation> citationsByAuthor = getRecallFromCitationIndex(citation, false, true);
			List<Citation> result = new ArrayList<Citation>();
			for(Citation c: citationsByTitle){
				long root = citationNetworkService.getRootByCitationId(c.getCitationId());
				LOGGER.info("root="+root);
				if(root == -1 || (rootSet.contains(root) || rootSet.contains(c.getCitationId())))
					continue;
				rootSet.add(root);
				rootSet.add(c.getCitationId());
				if(c.getCitationId() != citationId && FuzzyComparison.isTitleFuzzyEqual(citation.getTitle(), c.getTitle(), 0.8f))
					result.add(c);
			}
			for(Citation c: citationsByAuthor){
				long root = citationNetworkService.getRootByCitationId(c.getCitationId());
				LOGGER.info("root="+root);
				if(root == -1 || (rootSet.contains(root) || rootSet.contains(c.getCitationId())))
					continue;
				rootSet.add(root);
				rootSet.add(c.getCitationId());
				if(c.getCitationId() != citationId && CitationArticleComparison.isCitationCitationAuthorListEqual(citation.getAuthors(), c.getAuthors()))
					result.add(c);
			}
			if(result.size() >0){
				index++;
				samples.put(citation, result);
				LOGGER.info(index+" sampled");
				if(samples.size() == sampleSize)
					break;
			}
		}
		return samples;
	}
	
	public int getSampleSize() {
		return sampleSize;
	}

	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}
	
	public int calcualateTotal(int people, int numPerPeople, int overlap){
		int total = (numPerPeople- overlap)* (people -1) + numPerPeople;
		return total;
	}
	
	public void generateFilesForEachAnnotator(String save,
			HashMap<Citation, List<ElsevierArticle>> citationArticleMap, 
			List<Citation> citationArticleKeyList,
			HashMap<Citation, List<Citation>> citationCitationMap,
			List<Citation> citationCitationKeyList,
			HashMap<ElsevierArticle, List<PMArticle>> elsPMMap,
			List<ElsevierArticle> elsPMKeyList
			){
		
		LOGGER.info("Writing page for "+save);
		try {
			FileWriter keyWriter = new FileWriter(save+"key.txt");
			FileWriter htmlWriter = new FileWriter(save+".html");
			StringBuffer sb = new StringBuffer();
			for(Citation c: citationArticleKeyList){
				sb.append(CITATION_ARTICLE_ID).append(c.getCitationId()).append('\n');
			}
			for(Citation c: citationCitationKeyList){
				sb.append(CIATION_CITATION_ID).append(c.getCitationId()).append('\n');
			}
			for(ElsevierArticle els: elsPMKeyList){
				sb.append(ELSEVIER_MEDLINE_ID).append(els.getArticleId()).append('\n');
			}
			keyWriter.append(sb.toString());
			keyWriter.close();
			
			
			String citationArticleTable = getHTMLForCitationArticleCandidatesRow(citationArticleMap, new HashSet<Citation>(citationArticleKeyList));
			String elsPMTable = getHTMLForElsevierMedlineCandidateRow(elsPMMap, new HashSet<ElsevierArticle>(elsPMKeyList));
			String citationCitationTable = getHTMLForCitationCitationCandidatesRow(citationCitationMap, new HashSet<Citation>(citationCitationKeyList));
			StringBuffer page = new StringBuffer();
			page.append("<table border=1 >");
			page.append("<tr><td colspan=4>CITATION-ARTICLE MAPPING</td></tr>");
			page.append(citationArticleTable);
			page.append("<tr><td colspan=4>ELSEVIER-MEDLINE MAPPING</td></tr>");
			page.append(elsPMTable);
			page.append("<tr><td colspan=4>CITATION-CITATION MAPPING</td></tr>");
			page.append(citationCitationTable);
			page.append("</table>");
			htmlWriter.append(page.toString());
			htmlWriter.close();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
			
		
	}

	public void generateFileForAnnotators(String saveRoot,
			int people, int numPerPeople, int overlap
			) throws SQLException, IOException, ClassNotFoundException, ParseException{
		
		int total  =calcualateTotal(people, numPerPeople, overlap);
		sampleSize = total;
		LOGGER.info("Start generating file for annotators. people="+people+", each has="+numPerPeople+", overlap="+overlap);
		HashMap<Citation, List<ElsevierArticle>> citationArticleMap = getArticlesByCitations();
		HashMap<Citation, List<Citation>> citationCitationMap = getCitationsByCitations();
		HashMap<ElsevierArticle, List<PMArticle>> elsPMMap = getPMArticlesByElsevier();
		
		Set<Citation> citationArticleKeys = citationArticleMap.keySet();
		List<Citation> citationArticleKeyList = new ArrayList<Citation>(citationArticleKeys);
		Set<Citation> citationCitationKeys = citationCitationMap.keySet();
		List<Citation> citationCitationKeyList = new ArrayList<Citation>(citationCitationKeys);
		Set<ElsevierArticle> elsPMKeys = elsPMMap.keySet();
		List<ElsevierArticle> elsPMKeyList = new ArrayList<ElsevierArticle>(elsPMKeys);
		
		int p = 0;
		int start = 0;
		int end =  start+ numPerPeople;
		while(p< people){
			LOGGER.info("Generating files for annotator "+p);
			if(p==0){
				List<Citation> casub = citationArticleKeyList.subList(0, numPerPeople);
				List<Citation> ccsub = citationCitationKeyList.subList(0, numPerPeople);
				List<ElsevierArticle> epsub = elsPMKeyList.subList(0, numPerPeople);
				generateFilesForEachAnnotator(saveRoot+p, citationArticleMap, casub, citationCitationMap, ccsub, elsPMMap, epsub);
				end = numPerPeople;
				p++;
			}else{
				start = end -overlap;
				end = start+ numPerPeople;
				List<Citation> casub = citationArticleKeyList.subList(start, end);
				List<Citation> ccsub = citationCitationKeyList.subList(start, end);
				List<ElsevierArticle> epsub = elsPMKeyList.subList(start, end);
				generateFilesForEachAnnotator(saveRoot+p, citationArticleMap, casub, citationCitationMap, ccsub, elsPMMap, epsub);
				p++;
			}
		}
		LOGGER.info("All the "+people+" annotators have been generated");
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		if(args.length ==0 ){
//			System.out.println("--save  -sample-size(optional)");
//			return;
//		}
		if(args.length!= 4){
			System.out.println("--saveRoot  --people --numPerPeople --overlap");
			return;
		}
		try {
//			RecallSampleGenerator gen = new RecallSampleGenerator();
//			if(args.length==2){
//				gen.setSampleSize(Integer.parseInt(args[1]));
//			}
//			gen.getRecallSampleEvaluationTable(args[0]);
			
			RecallSampleGenerator gen = new RecallSampleGenerator();
			gen.generateFileForAnnotators(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
			
		
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
