/**
 * 
 */
package edu.uwm.elsevier.misc;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.article.Organization;
import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.CitationArticleComparison;
import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.evaluation.Sampler;
import edu.uwm.elsevier.misc.AuthorOrgnizationSample.AuthorInfo;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.namedisambiguation.AuthorityTool;

/**
 * @author qing
 *
 */
public class AuthorOrgnizationSample {

	private int authorCount = 4680057;
	private ArticlesDataDBConnection databaseConnection;
	private AuthorDSBService authorDSBService ;
	private CitationNetworkService citationNetworkService;
	private AuthorService authorService;
	private Logger logger = Logger.getLogger(AuthorOrgnizationSample.class);
	private String saveDir = "/Users/qing/dev/author_org_experiement/";
	
	public AuthorOrgnizationSample() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
		authorDSBService = new AuthorDSBService();
		authorService = new AuthorService();
		citationNetworkService = new CitationNetworkService();
	}
	
	public void getAuthorInfos(int size, String save) throws SQLException, IOException{
		Set<String> authors = sampleAuthorityId(size);
		FileWriter writer = new FileWriter(save);
		FileWriter debug = new FileWriter(save+"_debug");
		int count =1;
		for(String author: authors){
			logger.info("Author "+count+":"+author);
			List<AuthorInfo> infos = getAuthorInfo(author);
			String summary = summaryAuthor(infos);
			writer.append("AUTHOR "+count+":"+author).append('\n');
			writer.append(summary).append('\n');
			
			//output debug information to check the calculation
			String simple = summaryAuthorSimple(infos);
			debug.append("AUTHOR "+count+":"+author).append('\n');
			debug.append(simple).append('\n');
			count++;
		}
//		logger.info("Generating author infos for figures ...");
//		getAuthorInfosForFigure(authors);
		writer.close();
		debug.close();
		logger.info("Task done.");
	}
	
	public void getAuthorInfosForFigure(Set<String> authors) throws SQLException, IOException{
		int count =1;
		FileWriter writer;
		for(String author: authors){
			writer = new FileWriter(saveDir+count+".txt");
			List<AuthorInfo> infos = getAuthorInfo(author);
			String summary = summaryAuthorForFigure(infos);
			writer.append(summary);
			writer.close();
			count++;
		}
	}
	
	public String summaryAuthor(List<AuthorInfo> infos){
		AuthorInfo[] array = new AuthorInfo[infos.size()];
		infos.toArray(array);
		Arrays.sort(array);
		StringBuffer sb = new StringBuffer();
		int curYear = 0;
		AuthorInfo info = null;
		for(AuthorInfo author: array){
			if(curYear==0){
				info = new AuthorInfo();
				curYear =author.year;
			}else if(author.year != curYear){
				sb.append(info.toString()).append('\n');
				info = new AuthorInfo();
				curYear = author.year;
			}
			info.authorityId = author.authorityId;
			info.orgSame += author.orgSame;
			info.orgDiff += author.orgDiff;
			info.year =curYear;
		}
		if(info!= null)
			sb.append(info.toString()).append('\n');
		return sb.toString();
	}
	
	public String summaryAuthorForFigure(List<AuthorInfo> infos){
		AuthorInfo[] array = new AuthorInfo[infos.size()];
		infos.toArray(array);
		Arrays.sort(array);
		StringBuffer sb = new StringBuffer();
		int curYear = 0;
		AuthorInfo info = null;
		for(AuthorInfo author: array){
			if(curYear==0){
				info = new AuthorInfo();
				curYear =author.year;
			}else if(author.year != curYear){
				if(!(info.orgDiff +info.orgSame ==0))
					sb.append(info.toStatString()).append('\n');
				info = new AuthorInfo();
				curYear = author.year;
			}
			info.authorityId = author.authorityId;
			info.orgSame += author.orgSame;
			info.orgDiff += author.orgDiff;
			info.year =curYear;
		}
		if(!(info.orgDiff +info.orgSame ==0))
			sb.append(info.toStatString()).append('\n');
		return sb.toString();
	}
	
	public List<AuthorInfo> summaryByYear(List<AuthorInfo> infos){
		AuthorInfo[] array = new AuthorInfo[infos.size()];
		infos.toArray(array);
		Arrays.sort(array);
		List<AuthorInfo> byYearList = new ArrayList<AuthorOrgnizationSample.AuthorInfo>();
		int curYear = 0;
		AuthorInfo info = null;
		for(AuthorInfo author: array){
			if(curYear==0){
				info = new AuthorInfo();
				curYear =author.year;
			}else if(author.year != curYear){
				if(!(info.orgDiff +info.orgSame ==0))
					byYearList.add(info);
				info = new AuthorInfo();
				curYear = author.year;
			}
			info.authorityId = author.authorityId;
			info.orgSame += author.orgSame;
			info.orgDiff += author.orgDiff;
			info.year =curYear;
		}
		byYearList.add(info);
		return byYearList;
	}
	
	public String summaryAuthorSimple(List<AuthorInfo> infos){
		AuthorInfo[] array = new AuthorInfo[infos.size()];
		infos.toArray(array);
		Arrays.sort(array);
		StringBuffer sb = new StringBuffer();
		for(AuthorInfo info: array){
			sb.append(info.toString()).append('\n');
		}
		return sb.toString();
	}
	
	public List<AuthorInfo> getAuthorInfo(String authorityId) throws SQLException{
		String sql = "select pmid, author_id from "+ITableNames.AUTHORITY_MAP+" where authority_author_id=\'"+AuthorityTool.escape(authorityId)+"\'";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		List<AuthorInfo> infos = new ArrayList<AuthorInfo>();
//		logger.info("Processing "+authorityId);
		while(rs.next()){
			long pmid = rs.getLong(1);
			List<Integer> articleIds = citationNetworkService.getArticleIdByPMID(pmid);
			if(articleIds.size()>1){
				continue;
			}
			List<Author> authors = authorService.getAuthorListByArticleId(articleIds.get(0));
			List<Organization> srcs = authorService.getOrganizationsByAuthorId(rs.getInt(2));
			List<Organization> dests = new ArrayList<Organization>();
			for(Author author: authors){
				if(author.getAuthorId()!=rs.getInt(2))
					dests.addAll(authorService.getOrganizationsByAuthorId((int)author.getAuthorId()));
			}
			
			int same =0;
			int destIndex = 1;
			for(Organization dest: dests){
//				System.out.println("dest"+destIndex+":"+dest.getName());
				for(Organization src: srcs){
//					System.out.println("src:"+src.getName());
					if(CitationArticleComparison.isTitleEqual(dest.getName(), src.getName())){ // use title comparison method to compare institution names
						same++;
//						System.out.println("SAME "+same);
						break;
					}
				}
				destIndex++;
			}
			int diff = dests.size()-same;
			if(diff <0)
				logger.error("dest:same="+dests.size()+":"+same);
			AuthorInfo info = new AuthorInfo();
			info.authorityId = authorityId;
			info.orgDiff = diff;
			info.orgSame = same;
			info.year = citationNetworkService.getYearByPMID(pmid);
			infos.add(info);
//			System.out.println(authorityId+":"+info.year+"\tdest:"+dests.size()+"\tsame:"+same);
		}
		return infos;
	}
	
	public void getAuthorInfoAllForDB(){
		
	}
	
	public class AuthorInfo implements Comparable<AuthorInfo>{
		String authorityId;
		protected int orgSame;
		protected int orgDiff;
		protected int year;
		
		@Override
		public String toString(){
			StringBuffer sb = new StringBuffer();
			sb.append(authorityId).append('\t').append(orgSame).append('\t').append(orgDiff).append('\t').append(String.valueOf(orgDiff*1.0f/(orgSame+orgDiff))).append('\t').append(year);
			return sb.toString();
		}

		@Override
		public int compareTo(AuthorInfo o) {
			if(year > o.year)
				return 1;
			else if(year < o.year)
				return -1;
			return 0;
		}
		
		public String toStatString(){
			StringBuffer sb = new StringBuffer();
			sb.append(String.valueOf(orgDiff*1.0f/(orgSame+orgDiff))).append('\t').append(year);
			return sb.toString();
		}

		public String getAuthorityId() {
			return authorityId;
		}

		public void setAuthorityId(String authorityId) {
			this.authorityId = authorityId;
		}

		public int getOrgSame() {
			return orgSame;
		}

		public void setOrgSame(int orgSame) {
			this.orgSame = orgSame;
		}

		public int getOrgDiff() {
			return orgDiff;
		}

		public void setOrgDiff(int orgDiff) {
			this.orgDiff = orgDiff;
		}

		public int getYear() {
			return year;
		}

		public void setYear(int year) {
			this.year = year;
		}
		
		
	}
	
	public Set<String> sampleAuthorityId(int total) throws SQLException{
		logger.info("Sampling for "+total+" authors");
		Set<String> authorityids = new HashSet<String>();
		int i=1;
		while(authorityids.size() <total ){
			HashSet<Integer> seeds = Sampler.getSeeds(i*total, authorCount);
			for(Integer seed: seeds){
				String authorityId = getAuthorityIdFromPosition(seed);
				authorityids.add(authorityId);
			}
			i++;
		}
		return authorityids;
	}
	
	public String getAuthorityIdFromPosition(int pos) throws SQLException{
		String sql = "select authority_author_id from "+ITableNames.AUTHORITY_MAP+" limit "+pos+",1";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs= stmt.executeQuery(sql);
		String authorityId = null;
		if(rs.next()){
			authorityId = rs.getString(1);
		}
		return authorityId;
	}
	
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NumberFormatException, IOException {
		if(args.length != 2){
			System.out.println("--sample-size --save");
			return;
		}
		AuthorOrgnizationSample aos = new AuthorOrgnizationSample();
		aos.getAuthorInfos(Integer.parseInt(args[0]), args[1]);
	}

}
