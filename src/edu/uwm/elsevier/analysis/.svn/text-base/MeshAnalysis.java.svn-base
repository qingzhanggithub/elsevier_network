/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import pmidmapper.MedlineSearcher;

import articlesdata.citation.CitationParserUtils;
import articlesdata.database.ArticlesDataDBConnection;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.DatabaseSerivices;
import edu.uwm.elsevier.ITableNames;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

/**
 * @author qing
 *
 */
public class MeshAnalysis {
	private int maxArticleId = 25438518;
	private static String INSERT_MAP = "insert into "+ITableNames.MESH_ARTICLE_ID_MAP+
			" (mesh, article_ids) values (?, ?)";
	private static String SELECT_MAP = "select mesh, article_ids from "+ITableNames.MESH_ARTICLE_ID_MAP;
	private PreparedStatement prepInsertMap ;
	private ArticlesDataDBConnection databaseConnection;
	private CitationNetworkService citationNetworkService;
	private static Logger LOGGER = Logger.getLogger(MeshAnalysis.class);
	private SimpleDateFormat simpleDateformat=new SimpleDateFormat("yyyy");
	private final String[] listA = 
			new String[]{"Schizophrenia","Alzheimer Disease","Obesity","Hypertension","Parkinson Disease","HIV-1"};
	//"Stem Cells", "Smoking","DNA","SARS Virus"
	private final String[] listB =
			new String[]{"Influenza A Virus, H1N1 Subtype", "Influenza A Virus, H5N1 Subtype"};
	public MeshAnalysis() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
		citationNetworkService = new CitationNetworkService();
		prepInsertMap = databaseConnection.getConnection().prepareStatement(INSERT_MAP);
	}
	
	
	public void getMeshDistOverCitation(String saveRoot) throws SQLException, IOException{
		LOGGER.info("get mesh distribution over citation...");
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stmt.executeQuery(SELECT_MAP);
		FileWriter eWriter = new FileWriter(saveRoot+"mesh_eincite.csv");
		FileWriter mWriter = new FileWriter(saveRoot+"mesh_mincite.csv");
		int index =0;
		while(rs.next()){
			String mesh =rs.getString(1);
			String idStr = rs.getString(2);
			Set<Integer> idSet = parseArticleIds(idStr);
			int einciteCount =0;
			int minciteCount =0;
			for(int articleId : idSet){
				MedlineAritcleNodeStatistiscs stats = 
						citationNetworkService.getMedlineAritcleNodeStatistiscsByArticleId(articleId);
				einciteCount += stats.getIncites();
				minciteCount += stats.getIncitesFromMedline();
			}
			einciteCount = einciteCount/idSet.size();	//normalization
			minciteCount = minciteCount/idSet.size();	// normalization
			eWriter.append(mesh).append('\t').append(String.valueOf(einciteCount)).append('\n');
			mWriter.append(mesh).append('\t').append(String.valueOf(minciteCount)).append('\n');
			index++;
			
			LOGGER.info(index +"processed.");
		}
		eWriter.close();
		mWriter.close();
		rs.clearWarnings();
		stmt.close();
		
	}
	
	
	public void getMeshDistOverCitation(MeshEntity entity) throws SQLException, ClassNotFoundException{
//		LOGGER.info("mesh_id:"+entity.getMeshId()+"\narticleIds:"+entity.getArticleIds());
		Set<Integer> idSet = parseArticleIds(entity.getArticleIds());
		int einciteCount =0;
		int minciteCount =0;
		StringBuffer eyearSb = new StringBuffer();
		StringBuffer myearSb = new StringBuffer();
		for(int articleId : idSet){
//			LOGGER.info("checking articleId:"+articleId);
			MedlineAritcleNodeStatistiscs stats = citationNetworkService.getMedlineAritcleNodeStatistiscsByArticleId(articleId);
			einciteCount += stats.getIncites();
			minciteCount += stats.getIncitesFromMedline();
//			eyearSb.append(getEinciteYears(articleId));
			myearSb.append(getMinciteYears(articleId));
		}
		entity.setEinciteCount(einciteCount);
		entity.setMinciteCount(minciteCount);
		entity.setIdSetSize(idSet.size());
//		entity.setEinciteYears(eyearSb.toString());
		entity.setMinciteYears(myearSb.toString());
	}
	
	public void getMeshCitationOverYearByMeshId(int meshId) throws SQLException, ClassNotFoundException, IOException{
		LOGGER.info("Getting year info for mesh:"+meshId);
		String sql = "select mesh, article_ids from "+ ITableNames.MESH_ARTICLE_ID_MAP+" where mesh_id="+meshId;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		MeshEntity entity = new MeshEntity();
		entity.setMeshId(meshId);
		while(rs.next()){
			String mesh = rs.getString("mesh");
			String article_ids = rs.getString("article_ids");
			entity.setMesh(mesh);
			entity.setArticleIds(article_ids);
			getMeshDistOverCitation(entity);
			writeMeshYear(entity);
		}
		rs.close();
		stmt.close();
	}
	
	public void genCitationOverYearForInterestGroups(String listName) throws SQLException, ClassNotFoundException, IOException{
		LOGGER.info("Getting interest ones:");
		String[] list = null;
		if(listName.equals("A"))
			list = listA;
		else
			list = listB;
		String sql = "select mesh_id from "+ITableNames.MESH_STATS+" where mesh=";
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs =null;
		for(String mesh: list){
			LOGGER.info("Getting year info for mesh:"+mesh);
			rs = stmt.executeQuery(sql+"\'"+mesh+"\'");
			if(rs.next()){
				int meshId = rs.getInt(1);
				getMeshCitationOverYearByMeshId(meshId);
			}else
				LOGGER.error("No meshId found for mesh:"+mesh);
		}
		rs.close();
		stmt.close();
	}
	
	public void writeMeshYear(MeshEntity entity) throws IOException{
		FileWriter writer = new FileWriter("/home/qzhang/network_analysis/interest/bmesh"+entity.getMeshId()+".csv");
		String years = entity.getMinciteYears();
		if(years ==null)
			return;
		String[] words = years.split(",");
		HashMap<String, Integer> freq = new HashMap<String, Integer>();
		for(String word: words){
			Integer num = freq.get(word);
			if(num ==null){
				num =1;
			}else
				num= num+1;
			freq.put(word, num);
		}
		for(String key: freq.keySet()){
			writer.append(key).append("\t").append(String.valueOf(freq.get(key))).append('\n');
		}
		writer.close();
	}
	
	public static String getEinciteYears(int articleId) throws SQLException, ClassNotFoundException{
		CitationNetworkService service =DatabaseSerivices.getCitationNetworkService();
		List<Integer> einciteArticleIds = service.getInciteArticleIdsForArticleId(articleId);
		List<Date> dates = service.getArticleYearByArticleIds(einciteArticleIds);
		StringBuffer sb = new StringBuffer();
//		LOGGER.info("datesize="+dates.size());
		int empty =0;
		for(Date date: dates){
			if(date ==null){
				empty++;
				continue;
			}
			int y = CitationParserUtils.getYear(date.toString());
//			System.out.println("y="+y);
			if(y!=0)
				sb.append(y).append(',');
			else
				empty++;
		}
//		LOGGER.info("eempty="+empty);
		return sb.toString();
	}
	
	public static String getMinciteYears(int articleId) throws SQLException, ClassNotFoundException{
		CitationNetworkService service =DatabaseSerivices.getCitationNetworkService();
		List<Integer> minciteArticleIds = service.getInciteMedlineArticleIdsForArticleId(articleId);
		StringBuffer sb = new StringBuffer();
		for(int marticleId : minciteArticleIds){
			MedlineAritcleNodeStatistiscs stats = 
					service.getMedlineAritcleNodeStatistiscsByArticleId(marticleId);
			if(stats.getYear()!= 0)
				sb.append(stats.getYear()).append(",");
		}
		return sb.toString();
	}
	
	
	private Set<Integer> parseArticleIds(String ids){
		Set<Integer> idSet = new HashSet<Integer>();
		if(ids == null)
			return idSet;
		String[] idList = ids.split(",");
		for(String idStr : idList){
			idSet.add(Integer.parseInt(idStr));
		}
		return idSet;
	}

	public void createMeshArticleIdMapping() throws SQLException{
		LOGGER.debug("Creating mesh map table");
		Map<String, Set<Integer>> meshMap = new HashMap<String, Set<Integer>>();
		String sql = "select article_id, meshs from "+ ITableNames.MEDLINE_NETWORK_STAT_TABLE;
		Statement stmt = databaseConnection.getConnection().createStatement();
		int lower = 0;
		int page = 10000;
		int upper = lower + page;
		ResultSet rs;
		while(lower < maxArticleId){
			LOGGER.info("lower="+lower);
			rs= stmt.executeQuery(sql+" where article_id>="+lower+" and article_id<"+upper);
			while(rs.next()){
				int articleId = rs.getInt(1);
				List<String> meshList = MedlineSearcher.parseMeshs(rs.getString(2));
				putToMap(articleId, meshList, meshMap);
			}
			lower = upper;
			upper += page;
		}
		LOGGER.info("Mash map created. now writing it to db...");
		writeMashMapToDB(meshMap);
		
	}
	
	public void closeAllStuff() throws SQLException{
		if(databaseConnection !=null)
			databaseConnection.close();
	}
	
	private void putToMap(int articleId, List<String> meshList, Map<String, Set<Integer>> meshMap){
		for(String mesh: meshList){
			Set<Integer> ids = meshMap.get(mesh);
			if(ids ==null){
				ids = new HashSet<Integer>();
				meshMap.put(mesh, ids);
			}
			ids.add(articleId);
		}
	}
	
	
	
	private void writeMashMapToDB(Map<String, Set<Integer>> meshMap) throws SQLException{
		LOGGER.info("writing Mash map to db...");
		Set<String> keys = meshMap.keySet();
		for(String key: keys){
			Set<Integer> articleIds = meshMap.get(key);
			StringBuffer sb =new  StringBuffer();
			boolean isFirst = true;
			for(int id: articleIds){
				if(isFirst){
					sb.append(id);
					isFirst= false;
				}else{
					sb.append(",").append(id);
				}
			}
			String idStr = sb.toString();
			insertMap(key, idStr);
			
		}
		LOGGER.info("Finished writting to db.");
	}
	
	private void insertMap(String mesh, String idStr) throws SQLException{
		prepInsertMap.setString(1, mesh);
		prepInsertMap.setString(2, idStr);
		prepInsertMap.executeUpdate();
	}
	
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NumberFormatException, IOException {
		try {
			MeshAnalysis ma = new MeshAnalysis();
			ma.genCitationOverYearForInterestGroups(args[0]);
			ma.closeAllStuff();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
//		if(args.length ==0){
//			System.out.println("--mesh_ids ");
//			return;
//		}
//		MeshAnalysis ma = new MeshAnalysis();
//		for(String arg: args){
//			ma.getMeshCitationOverYearByMeshId(Integer.parseInt(arg));
//		}
	}

}
