package edu.uwm.elsevier.prediction;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.queryParser.ParseException;

import articlesdata.article.ArticleService;
import articlesdata.article.Author;
import articlesdata.article.Citation;
import articlesdata.article.Organization;

import pmidmapper.MedlineSearcher;
import pmidmapper.PMArticle;
import edu.uwm.elsevier.CitationArticleComparison;
import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ElsevierArticleMetaDataSearcher;
import edu.uwm.elsevier.authoranalysis.InterDisipline;
import edu.uwm.elsevier.indexer.ElsevierCitationIndexAccess;
import edu.uwm.elsevier.indexer.ElsevierCitationIndexer;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;


public class SRWFeatureExtraction {
	
	private List<AuthorshipEdge> trainingEdges;
	private MedlineSearcher medlineSearcher;
	private SRWCollaborationGraph graph;
	private AuthorDSBService authorDSBService;
	private InterDisipline interDisipline;
	private CitationNetworkService citationNetworkServcie;
	private ArticleService articleService;
	private ElsevierCitationIndexAccess citationIndexAccess;
	private CoauthorJDBC coauthorJDBC;
	public static int DEAULT_YEAR_CUTOFF = 2005;
	private int endYear= -1;
	private int startYear = -1;
	
	public SRWFeatureExtraction(int endYear) throws ClassNotFoundException, SQLException, IOException{
		medlineSearcher = new MedlineSearcher(MedlineSearcher.defaultMedlineIndexPath);
		authorDSBService = new AuthorDSBService();
		citationNetworkServcie = new CitationNetworkService();
		citationIndexAccess = new ElsevierCitationIndexAccess(ElsevierCitationIndexer.CITATION_TEXT_FIELD);
		articleService = new ArticleService();
		coauthorJDBC = new CoauthorJDBC(endYear);
		this.endYear = endYear;
	}
	
	/**
	 * Get number of articles the author published before the year. It can be obtained by 
	 * summing up all the publications of the edges that linking to 
	 * this node (duplications need to be removed)
	 * @param authorityId
	 * @param year
	 * @return
	 * @throws SQLException 
	 */
	public int getNumOfArticlesPublishedBeforeYear(String authorityId) throws SQLException{
		return authorDSBService.getPmidsBetweenYearsByAuthorityId(authorityId, startYear, endYear).size();
	}
	
	
	public int getYearSpanOfAuthorBeforeYear(String authorityId) throws SQLException{
		List<Long> pmids = authorDSBService.getPmidsBetweenYearsByAuthorityId(authorityId, startYear, endYear);
		int min = endYear;
		int max = 0;
		for(long pmid: pmids){
			int year = citationNetworkServcie.getYearByPMID(pmid);
			if(year < min)
				min = year;
			if(year > max)
				max = year;
		}
		
		int span = max -min +1;
		return span;
	}
	/**
	 * Get how often the coauthorship occures
	 * @param coauthorship
	 * @return
	 */
	public float getFrequencyOfCoauthorship(AuthorshipEdge coauthorship){
		int small = 2012;
		int large = 0;
		for(PublicationAttribute pub: coauthorship.publications){
			if(pub.year <= small)
				small = pub.year;
			if(pub.year > large)
				large = pub.year;
		}
		
		float freq = coauthorship.publications.size()*1.0f/(large-small+1);	// the calculation is subject to change.
		System.out.println("pubsize:"+coauthorship.publications.size()+", endYear:"+large+", startYear:"+small+", freq="+freq);
		return freq;
	}
	/**
	 * A simple calculation of mesh consine without considering frequency. Overlap only. CHECKED.
	 * @param coauthorship
	 * @return
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws SQLException 
	 */
	public float getCosineOfMesh(AuthorshipEdge coauthorship) throws ParseException, IOException, SQLException{
		Set<Long> srcPmids = new HashSet<Long>(authorDSBService.getPmidsBetweenYearsByAuthorityId(coauthorship.src, startYear, endYear));
		Set<Long> destPmids = new HashSet<Long>(authorDSBService.getPmidsBetweenYearsByAuthorityId(coauthorship.dest, startYear, endYear));
		Set<String> srcMesh = new HashSet<String>();
		srcMesh = getMeshByPmids(srcPmids);
		Set<String> destMesh = new HashSet<String>();
		destMesh= getMeshByPmids(destPmids);
		int srcSize = srcMesh.size();
		int destSize = destMesh.size();
		if(srcSize ==0 || destSize ==0)
			return 0f;
		srcMesh.retainAll(destMesh);
		int overlap = srcMesh.size();
		float cosine =overlap*1.0f/(srcSize * destSize);
		System.out.println("overlap:"+overlap+"\tsrc::"+coauthorship.src+":"+srcSize+"\tdest::"+coauthorship.dest+":"+destSize+"\tcos:"+cosine);
		return cosine;
	}
	/**
	 * code checked.
	 * @param coauthorship
	 * @return
	 * @throws SQLException
	 */
	public int getNumOfCommonFriends(AuthorshipEdge coauthorship) throws SQLException{
		List<String> srcCo = authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(coauthorship.src, startYear, endYear);
		List<String> destCo = authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(coauthorship.dest, startYear, endYear);
		Set<String> srcSet = new HashSet<String>();
		srcSet.addAll(srcCo);
		Set<String> destSet = new HashSet<String>();
		destSet.addAll(destCo);
		int common = 0;
		for(String author: srcCo){
			if(!author.equalsIgnoreCase(coauthorship.dest) && destSet.contains(author))
				common ++;
		}
		return common;
	}
	
	public Map<String, Float> getInciteTFIDF(String authorId) throws SQLException, ParseException, IOException{
		List<Long> srcPMIDs = authorDSBService.getPmidsBetweenYearsByAuthorityId(authorId, startYear, endYear);
		List<Long> incites = new ArrayList<Long>();
		for(long pmid: srcPMIDs){
			List<Integer> articleIds = citationNetworkServcie.getArticleIdByPMID(pmid);
			if(articleIds.size() ==1){
				incites.addAll(citationNetworkServcie.getIncitePMIDForArticleIdBetweenYear(articleIds.get(0), startYear, endYear));
			}
		}
		Map<String, Float> tfidfs = interDisipline.getTFIDFForPMIDs(incites);
		return tfidfs;
	}
	
	public float getInciteCosine(AuthorshipEdge coauthorship) throws SQLException, ParseException, IOException{
		Map<String, Float> src = getInciteTFIDF(coauthorship.src);
		Map<String, Float> dest = getInciteTFIDF(coauthorship.dest);
		float cos = (float)InterDisipline.getCosine(src, dest);
		return cos;
	}
	
	public float getOutciteCosine(AuthorshipEdge coauthorship) throws SQLException, ParseException, IOException{
		Map<String, Float> src = getOutsiteTFIDF(coauthorship.src);
		Map<String, Float> dest = getOutsiteTFIDF(coauthorship.dest);
		float cos = (float)InterDisipline.getCosine(src, dest);
		return cos;
	}
	
	public Map<String, Float> getOutsiteTFIDF(String authorId) throws SQLException, ParseException, IOException{
		List<Long> pmids = authorDSBService.getPmidsBetweenYearsByAuthorityId(authorId, startYear, endYear);
		List<Long> citationIds = new ArrayList<Long>();
		for(long pmid: pmids){
			List<Integer> articleIds = citationNetworkServcie.getArticleIdByPMID(pmid);
			if(articleIds.size() ==1){
				List<Citation> citations = articleService.getCitationListByArticleId(articleIds.get(0));
				for(Citation c: citations)
					citationIds.add(c.getCitationId());
			}
		}
		Map<String, Float> tfidfs = citationIndexAccess.getTFIDFForCitationList(citationIds);
		return tfidfs;
	}
	
	public float getCumulatedRecency(AuthorshipEdge edge) throws SQLException{
		List<PublicationAttribute> pubList = coauthorJDBC.getPublicationForAuthorPair(edge, startYear, endYear);
		float recency = 0;
		for(PublicationAttribute pubAttr: pubList){
			int offset = endYear - pubAttr.year +1;
			recency += 1.0f /offset;
		}
		return recency;
	}
	
	
	public float getAuthorPositionSimilarity(AuthorshipEdge edge) throws SQLException{
		List<PublicationAttribute> pubList = coauthorJDBC.getPublicationForAuthorPair(edge, startYear, endYear);
		int positionSim = 0;
		if(pubList.size() ==0)		// if no publication history available, not same
			return positionSim;
		for(PublicationAttribute pub: pubList){
			Author src = coauthorJDBC.getAuthorByAuthorityIdAndPMID(edge.src, pub.pmid);
			Author dest = coauthorJDBC.getAuthorByAuthorityIdAndPMID(edge.dest, pub.pmid);
			if(src != null && dest!= null){
				if(!(src.isCorrespondingAuthor() || dest.isCorrespondingAuthor()))
					positionSim ++;
			}
		}
		return positionSim * 1.0f /pubList.size();
	}
	
	public float getInstituteSimilarity(AuthorshipEdge edge) throws SQLException{
		List<PublicationAttribute> pubList = coauthorJDBC.getPublicationForAuthorPair(edge, startYear, endYear);
		int instSame = 0;
		if(pubList.size() ==0)	// if no publication history available, not same
			return instSame;
		for(PublicationAttribute pub: pubList){
			Author src = coauthorJDBC.getAuthorByAuthorityIdAndPMID(edge.src, pub.pmid);
			Author dest = coauthorJDBC.getAuthorByAuthorityIdAndPMID(edge.dest, pub.pmid);
			if(src != null && dest!= null){
				List<Organization> srcOrgs = src.getOrganizations();
				List<Organization> destOrgs = dest.getOrganizations();
				boolean isEqual = false;
				for(Organization srcOrg: srcOrgs){
					for(Organization destOrg: destOrgs){
						isEqual = CitationArticleComparison.isTitleEqual(srcOrg.getName(), destOrg.getName());
						if(isEqual)
							break;
					}
					if(isEqual)
						break;
				}
				if(isEqual)
					instSame ++;
			}
		}
		
		return instSame * 1.0f /pubList.size();
	}
	
	public int getGeoDistance(AuthorshipEdge coauthorship){
		//TODO
		return -1;
	}
	/**
	 * 
	 * @return
	 */
	public int getInstitutionalTierDifference(){
		//TODO
		return -1;
	}
	
	public int getNumOfCoauthors(String authorityId) throws SQLException{
		int num = 0;
		num = authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityId, startYear, endYear).size();
		System.out.println("Num of coauthor of "+authorityId+"before "+endYear+":\t"+num);
		return num;
	}
	
	public float getCosineOfFullText(AuthorshipEdge coauthorship) throws SQLException, ParseException, IOException{
		System.out.println("Getting cosine for "+coauthorship.src+", "+coauthorship.dest);
		System.out.println("Src author: "+coauthorship.src);
		Map<String, Float> srcTFIDFMap = interDisipline.getTFIDFForAuthor(coauthorship.src);
		System.out.println("Dest author: "+coauthorship.dest);
		Map<String, Float> destTFIDFMap = interDisipline.getTFIDFForAuthor(coauthorship.dest);
		double cos = interDisipline.getCosine(srcTFIDFMap, destTFIDFMap);
		return (float) cos;
	}
	
	public Set<Long> getPMIDByAuthorityIdInTraingEdges(String authorityId){
		Set<Long> pmidSet = new HashSet<Long>();
		for(AuthorshipEdge authorship: trainingEdges){
			if(authorship.src.equalsIgnoreCase(authorityId) || authorship.dest.equalsIgnoreCase(authorityId)){
				for(PublicationAttribute pub: authorship.publications){
					pmidSet.add(pub.pmid);
				}
			}
		}
		return pmidSet;
	}
	
	public Set<String> getMeshByPmids(Set<Long> pmids) throws ParseException, IOException{
		Set<String> meshs = new HashSet<String>();
		for(Long pmid: pmids){
			PMArticle pmarticle = medlineSearcher.getPMArticleByPMID(pmid, false);
			String meshStr = pmarticle.getMeshs();
			meshs.addAll(MedlineSearcher.parseMeshs(meshStr));
		}
		return meshs;
	}
	
	public float getClusteringCoef(String authorityId) throws SQLException{
		float coef = authorDSBService.getClusteringCoefByAuthorityIdBetweenYears(authorityId, startYear, endYear);	// changed at 2/26/2013
		return coef;
	}
	
	
	
	public void extractFeatures(String authorityId, int hop , String save) throws ClassNotFoundException, SQLException, IOException, ParseException{
		System.out.println("Extracting features ...");
		graph = new SRWCollaborationGraph();
		endYear = graph.findYearByPercentile(authorityId, 0.8f);
		System.out.println("Median year ="+endYear);
		interDisipline = new InterDisipline(endYear);
		graph.BFSRadiator(authorityId, hop, endYear, -1); // CAREFUL!!!!
		graph.generateTrainingAndTestingGraph(endYear);
		trainingEdges = graph.getTrainingEdges();
		FileWriter writer = new FileWriter(save);
		int i=0;
		int size = trainingEdges.size();
		writer.append("src,dest,src_pub,dest_pub,collab_freq,mesh_cos,common_friends,src_coauthor,dest_coauthor").append("\n");
		for(AuthorshipEdge edge: trainingEdges){
			writer.append(edge.src).append(",").append(edge.dest).append(",");
			writer.append(String.valueOf(getNumOfArticlesPublishedBeforeYear(edge.src))).append(",");
			writer.append(String.valueOf(getNumOfArticlesPublishedBeforeYear(edge.dest))).append(",");
			writer.append(String.valueOf(getFrequencyOfCoauthorship(edge))).append(",");
			writer.append(String.valueOf(getCosineOfMesh(edge))).append(",");
//			writer.append(String.valueOf(getCosineOfFullText(edge))).append(",");
			writer.append(String.valueOf(getNumOfCommonFriends(edge))).append(",");
			writer.append(String.valueOf(getNumOfCoauthors(edge.src))).append(",");
			writer.append(String.valueOf(getNumOfCoauthors(edge.dest)));
			writer.append("\n");
			i++;
			if(i %100 == 0){
				System.out.println(i+"/"+size+" edge features have been processed.");
			}
		}
		writer.close();
		System.out.println("Finish extracting features .");
	}
	
	public void extractRandomWalkFeatures(String root, int hop, int start , int end, String save) throws ClassNotFoundException, SQLException, IOException, ParseException{
		System.out.println("Extracting features for random walk ...");
		graph = new SRWCollaborationGraph();
		endYear = start;
		interDisipline = new InterDisipline(start);
		Set<AuthorshipEdge> trainingGraph = graph.BFSRadiator(root, hop, -1, start);
		List<String> newCos = authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(root, start, end);
		Set<AuthorshipEdge> posSet = new HashSet<AuthorshipEdge>();
		Set<AuthorshipEdge> negSet = new HashSet<AuthorshipEdge>();
		
		for(String co: newCos){
			posSet.add(new AuthorshipEdge(root, co));	// getting features before Year start	
		}
		for(AuthorshipEdge edge: trainingGraph){
			AuthorshipEdge toSrc = new AuthorshipEdge(root, edge.src);
			AuthorshipEdge toDest = new AuthorshipEdge(root, edge.dest);
			if(!trainingGraph.contains(toSrc) && !posSet.contains(toSrc)){
				negSet.add(toSrc);
			}
			if(!trainingGraph.contains(toDest) && !posSet.contains(toDest))
				negSet.add(toDest);
		}
		System.out.println("Size of new edges after "+start+": "+newCos.size()+". Size of negs: "+negSet.size());
		FileWriter writer = new FileWriter(save);
//		writer.append("src,dest,sum_pub,mesh_cos,fulltext_cos,common_friends,sum_coauthor,clustering_coef,connect").append("\n");
//		writeFeatures(posSet, "pos", writer);
//		writeFeatures(negSet, "neg", writer);
		
		writer.append("src,dest,connect").append("\n");
		writeFeatures(posSet, "pos", writer);
		writeFeatures(negSet, "neg", writer);
		writer.close();
	}
	
	private void writeFeatures(Set<AuthorshipEdge> set, String cls, FileWriter writer) throws IOException, SQLException, ParseException{
		for(AuthorshipEdge edge: set){
			writer.append(edge.src).append(",").append(edge.dest).append(",");
//			writer.append(String.valueOf(getNumOfArticlesPublishedBeforeYear(edge.src)+getNumOfArticlesPublishedBeforeYear(edge.dest))).append(",");
//			writer.append(String.valueOf(getCosineOfMesh(edge))).append(",");
//			writer.append(String.valueOf(getCosineOfFullText(edge))).append(",");
//			writer.append(String.valueOf(getNumOfCommonFriends(edge))).append(",");
//			writer.append(String.valueOf(getNumOfCoauthors(edge.src)+getNumOfCoauthors(edge.dest))).append(",");
//			writer.append(String.valueOf(getClusteringCoef(edge.src) + getClusteringCoef(edge.dest))).append(',');
			writer.append(cls).append("\n");
		}
	}
	
	public List<AuthorshipEdge> getTrainingEdges() {
		return trainingEdges;
	}

	public void setTrainingEdges(List<AuthorshipEdge> trainingEdges) {
		this.trainingEdges = trainingEdges;
	}
	

	public int getEndYear() {
		return endYear;
	}

	public void setEndYear(int yearCutoff) {
		this.endYear = yearCutoff;
	}
	
	

	public int getStartYear() {
		return startYear;
	}

	public void setStartYear(int startYear) {
		this.startYear = startYear;
	}

	public InterDisipline getInterDisipline() {
		return interDisipline;
	}

	public void setInterDisipline(InterDisipline interDisipline) {
		this.interDisipline = interDisipline;
	}
	
	public void closeAllStuff() throws SQLException, IOException{
		if(authorDSBService!=null)
			authorDSBService.closeService();
		if(citationNetworkServcie !=null)
			citationNetworkServcie.closeService();
		if(medlineSearcher !=null)
			medlineSearcher.close();
		if(coauthorJDBC !=null)
			coauthorJDBC.closeDBConnection();
		if(citationIndexAccess !=null)
			citationIndexAccess.close();
			
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException{
		if(args.length != 5){
			System.out.println("--source-node --hop --start --end --save");
			return;
		}
		SRWFeatureExtraction extraction = new SRWFeatureExtraction(Integer.parseInt(args[2]));
		extraction.extractRandomWalkFeatures(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]), args[4]);
		extraction.closeAllStuff();
	}
}
