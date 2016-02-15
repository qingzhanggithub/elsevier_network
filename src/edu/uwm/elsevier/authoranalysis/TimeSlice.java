package edu.uwm.elsevier.authoranalysis;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import articlesdata.database.ArticlesDataDBConnection;
import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;
import edu.uwm.elsevier.misc.AuthorOrgnizationSample;
import edu.uwm.elsevier.misc.AuthorOrgnizationSample.AuthorInfo;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;

public class TimeSlice {
	private ArticlesDataDBConnection articleDatabaseConnection;
	private AuthorDSBService authorDSBService;
	private CitationNetworkService citationNetworkService;
	private AuthorOrgnizationSample authorOrg;
	private Logger logger = Logger.getLogger(TimeSlice.class);
	
	public TimeSlice() throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
		citationNetworkService = new CitationNetworkService();
		authorOrg = new AuthorOrgnizationSample();
		articleDatabaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	/**
	 * Collect author information
	 * @param authorityId
	 * @throws SQLException
	 */
	public List<TimeSliceEntity> getAuthorInfoAll(String authorityId) throws SQLException{
		List<Long> pmids = authorDSBService.getPmidsByAuthorityId(authorityId);	// using pmid is better. consistent
		Map<Integer, AuthorInfo> map = new HashMap<Integer, AuthorInfo>();
		List<TimeSliceEntity> timeSlices = new ArrayList<TimeSliceEntity>();
		for(long pmid: pmids){
			AuthorInfo info = new AuthorInfo();
			MedlineAritcleNodeStatistiscs stats = citationNetworkService.getMedlineAritcleNodeStatistiscsByPmid(pmid);
			info.year = stats.getYear();
			info.mincite =stats.getIncitesFromMedline();
			info.eincite = stats.getIncites();
			edu.uwm.elsevier.authoranalysis.TimeSlice.AuthorInfo entity = map.get(info.year);
			if(entity == null){
				entity = new AuthorInfo();
				map.put(info.year, entity);
			}
			entity.add(info); // put to the entity that has the corresponding year.
		}
		List<AuthorOrgnizationSample.AuthorInfo> authorOrgList = authorOrg.summaryByYear(authorOrg.getAuthorInfo(authorityId));
		int startYear = -1;
		for(AuthorOrgnizationSample.AuthorInfo org: authorOrgList){
			edu.uwm.elsevier.authoranalysis.TimeSlice.AuthorInfo entityArticle = map.get(org.getYear());
			if(entityArticle != null){ // consistency hold by author
				if(startYear == -1)
					startYear =org.getYear();
				int offset = org.getYear() - startYear;
				entityArticle.inititutePrecentage = org.getOrgDiff()*1.0f/(org.getOrgDiff()+org.getOrgSame());
				TimeSliceEntity entity = new TimeSliceEntity();
				entity.setAuthorityId(authorityId);
				entity.setYear(entityArticle.year);
				entity.setYearOffset(offset);
				entity.setMincite(entityArticle.mincite);
				entity.setEincite(entityArticle.eincite);
				entity.setCoAuthors(org.getOrgDiff()+org.getOrgSame());
				entity.setPrecentageInsititution(entityArticle.inititutePrecentage);
				entity.setNumPublications(entityArticle.numPublications);
				timeSlices.add(entity);
			}
		}
		return timeSlices;
	}
	
	public void processSliceOnSingleMachine(String save, int begin) throws SQLException, IOException{
		logger.info("Task start...");
		FileWriter writer = new FileWriter(save, true);
		String sql ="select distinct authority_author_id from "+ITableNames.AUTHORITY_MAP;
		Statement stmt = articleDatabaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(sql);
		int count =0;
		while(rs.next()){
			count ++;
			if(count<= begin)
				continue;
			List<TimeSliceEntity> entities = getAuthorInfoAll(rs.getString(1));
			for(TimeSliceEntity entity: entities){
				writer.append(entity.toString()).append('\n');
			}
			if(count %1000 ==0)
				logger.info("count:"+count);
		}
		writer.close();
		rs.close();
		stmt.close();
		logger.info("Task done. total:"+count);
	}
	
	public void closeAllStuff() throws SQLException{
		authorDSBService.closeService();
		citationNetworkService.closeService();
	}
	
	public class AuthorInfo{
		int mincite =0;
		int eincite = 0;
		int numOfCoAuthors=0;
		int coAuthorIntraInstitute =0;
		int coAuthorInterInstitute=0;
		int year =0;
		float inititutePrecentage = 0.0f;
		int numPublications = 0;
		
		public void add(AuthorInfo o){
			mincite += o.mincite;
			eincite += o.eincite;
			numOfCoAuthors += o.numOfCoAuthors;
			year = o.year;
			coAuthorIntraInstitute += o.coAuthorIntraInstitute;
			coAuthorInterInstitute += o.coAuthorInterInstitute;
			numPublications++;
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
//		String authorityId = "pilowsky_d_7583500_1";
//		List<TimeSliceEntity> infoList = slice.getAuthorInfoAll(authorityId);
//		System.out.println("==== OUTPUT ====");
//		for(TimeSliceEntity entity: infoList){
//			System.out.println(entity.toString());
//		}
		if(args.length !=2){
			System.out.println("--save --begin");
			return;
		}
		TimeSlice slice = new TimeSlice();
		try {
			slice.processSliceOnSingleMachine(args[0], Integer.parseInt(args[1]));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
