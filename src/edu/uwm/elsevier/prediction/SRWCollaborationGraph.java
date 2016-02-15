/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.analysis.YearAnalysis;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.namedisambiguation.AuthorityTool;

/**
 * @author qing
 *
 */
public class SRWCollaborationGraph {
	
	private AuthorDSBService authorDSBService;
	private ArticlesDataDBConnection articleDataDBConnection;
	private List<AuthorshipEdge> edges;
	private List<AuthorshipEdge> trainingEdges;
	private List<AuthorshipEdge> testingEdges;
	
	public SRWCollaborationGraph() throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
		articleDataDBConnection  = ArticlesDataDBConnection.getInstance();
	}
	
	public AuthorshipEdge getAuthorhipByPair(String src, String dest, int start, int end) throws SQLException{
		String sql = "select pmid, year from "+ ITableNames.CO_AUTHOR+" where authority_author_id=\'"+AuthorityTool.escape(src)+"\' and co_author_authority_author_id=\'"+AuthorityTool.escape(dest)+"\'";
		if(start != -1)
			sql +=" and year >="+start;
		if(end != -1)
			sql += " and year <"+end;
		Statement stmt = articleDataDBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<PublicationAttribute> publications = new ArrayList<PublicationAttribute>();
		while(rs.next()){
			long pmid = rs.getLong(1);
			int year = rs.getInt(2);
			PublicationAttribute publication = new PublicationAttribute(pmid, year);
			publications.add(publication);
		}
		AuthorshipEdge edge = new AuthorshipEdge(src, dest);
		edge.publications = publications;
		return edge;
	}
	
	
	public Set<AuthorshipEdge> BFSRadiator(String src, int hop, int begin, int end) throws SQLException{
		System.out.println("In radiator ...");
		Set<String> set = new HashSet<String>();
		set.add(src);
		Set<AuthorshipEdge> edges = new HashSet<AuthorshipEdge>();
		int layer = 0;
		
		List<String> startList = new ArrayList<String>();
		List<String> next = new ArrayList<String>();
		startList.add(src);
		while(layer < hop ){
			System.out.println("Processing layer : "+layer);
			for(String start: startList){
				if(hasSpace(start)){
					System.out.println(start+" has space. ignored.");
					continue;
				}
				System.out.println("starting from "+start);
				List<String> co = authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(start, begin, end);
				for(String dest: co){
					if(set.contains(dest))
						continue;
					if(hasSpace(dest)){
						System.out.println(dest+" has space. ignored.");
						continue;
					}
//					edges.add(getAuthorhipByPair(start, dest, begin, end));	// 4/22/2013 commented. becuase we don't need pmid and year info
					edges.add(new AuthorshipEdge(start, dest));
					next.add(dest);
					System.out.println("new edge: "+start+" -- "+dest+"("+begin+", "+end+")");
					set.add(dest);
				}
				co.clear();
			}
			System.out.println("Size of next :"+next.size());
			startList.clear();
			startList.addAll(next);
			next.clear();
			layer++;
		}
		System.out.println("Finish radiation.");
		return edges;
	}
	
	public static boolean hasSpace(String str){
		int size = str.length();
		for(int i=0; i<size; i++){
			if(str.charAt(i)==' ' || str.charAt(i)== '\t' || str.charAt(i)=='\'')
				return true;
		}
		return false;
	}
	
	
	public int findYearByPercentile(String src, float perc) throws SQLException{
		List<String> co = authorDSBService.getCoAuthorsByAuthorityId(src);
		List<Integer> years = new ArrayList<Integer>();
		for(String dest: co){
			AuthorshipEdge authorship = getAuthorhipByPair(src, dest, -1, -1);
			for(PublicationAttribute pub: authorship.publications){
				years.add(pub.year);
			}
		}
		Integer[] yearObjs = new Integer[years.size()];
		Arrays.sort(years.toArray(yearObjs));
		int mid = (int)Math.floor(years.size()*perc);
		return yearObjs[mid];
	}
	
	public void generateTrainingAndTestingGraph(int yearCutoff){
		System.out.println("Generating training /testing graph");
		trainingEdges = new ArrayList<AuthorshipEdge>();
		testingEdges = new ArrayList<AuthorshipEdge>();
		for(AuthorshipEdge authorship: edges){
			AuthorshipEdge train = new AuthorshipEdge(authorship.src, authorship.dest);
			AuthorshipEdge test = new AuthorshipEdge(authorship.src, authorship.dest);
			for(PublicationAttribute pub: authorship.publications){
				if(pub.year <=yearCutoff){
					train.publications.add(pub);
				}else{
					test.publications.add(pub);
				}
			}
			if(train.publications.size() >0)
				trainingEdges.add(train);
			if(test.publications.size() >0)
				testingEdges.add(test);
		}
		System.out.println("Finish.  training :"+trainingEdges.size()+"\t testing :"+testingEdges.size());
	}
	

	public List<AuthorshipEdge> getTrainingEdges() {
		return trainingEdges;
	}

	public List<AuthorshipEdge> getTestingEdges() {
		return testingEdges;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
