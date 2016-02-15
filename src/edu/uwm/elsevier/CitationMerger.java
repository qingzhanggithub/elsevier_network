/**
 * 
 */
package edu.uwm.elsevier;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import articlesdata.article.Author;
import articlesdata.article.Citation;
import articlesdata.citation.CitationService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class CitationMerger {

	private static Logger LOGGER = NetworkBuilderLogger.getLogger("CitationMerger");
	private static String INSERT_STATUS = "insert into cnetworkv4_citation_merger " +
			"(citation_id, target_citation_id, title_comp, authors_comp, journal_comp, is_matched, ddate)" +
			"value (?,?,?,?,?,?,?)";
	private static String SELECT_MERGER_STATUS = "select citation_id from cnetworkv4_citation_merger where citation_id= ?";
	private static String SELECT_CHECK_PAIR = "select citation_id from cnetworkv4_citation_merger " +
			"where (citation_id =? and target_citation_id = ?)";
	private static int GRACE_ZONE = 10;
	private PreparedStatement prepInsertMapping ;
	private PreparedStatement prepSelectMapping;
	private PreparedStatement prepSelectPair;
	private ArticlesDataDBConnection databaseConnection;
	private CitationSearcher searcher;
	
	public CitationMerger(String indexPath) throws IOException, ClassNotFoundException, SQLException{
		searcher = new CitationSearcher(indexPath);
//		databaseConnection = ArticlesDataDBConnection.getInstance();
//		prepInsertMapping = databaseConnection.getConnection().prepareStatement(INSERT_STATUS);
//		prepSelectMapping = databaseConnection.getConnection().prepareStatement(SELECT_MERGER_STATUS);
//		prepSelectPair = databaseConnection.getConnection().prepareStatement(SELECT_CHECK_PAIR);
	}
	
	
	public void margeCitation(Citation citation) throws IOException, SQLException{
		if(hasCitationChecked(citation.getCitationId()))
			return;
		List<Citation> citations = searcher.getCitations(citation);
		boolean inWindow = false;
		boolean found = false;
		int index = 0;
		MappingStatus status = new MappingStatus(citation.getCitationId());
		for(Citation target: citations){
			if(citation.getArticleId() == target.getArticleId()|| target.getCitationId() <= citation.getCitationId())
				continue;
			index++;
			status = new MappingStatus(citation.getCitationId());
			status.setIsMatched(0);
			CitationArticleComparison.isCitationAndCitationEqual(citation, target, status);
			if(status.getIsMatched()==1 ){
				found = true;
				status.setTargetCitationId(target.getCitationId());
				insertMappingStatus(status);
				if(!inWindow)
					inWindow = true;
			}else if(inWindow){//out of window, impossible to find new matched ones.
				inWindow = false;
				break;
			}
			if(index == GRACE_ZONE && !inWindow)
				break;
		}
		if(!found){
			status.setTargetCitationId(1);
			insertMappingStatus(status);// insert the default status.
		}
	}
	
	public List<CitationCitationMappingStatus> mergeCitationWithoutDB(Citation citation) throws IOException{
		
		List<Citation> citations = searcher.getCitations(citation);
		boolean inWindow = false;
		CitationCitationMappingStatus status = new CitationCitationMappingStatus(citation.getCitationId());
		List<CitationCitationMappingStatus> statusList = new ArrayList<CitationCitationMappingStatus>();
		int index = 0;
		for(Citation target: citations){
			if(citation.getCitationId() == target.getCitationId())
				continue;
			index++;
			status = new CitationCitationMappingStatus(citation.getCitationId());
			status.setIsMatched(0);
			CitationArticleComparison.isCitationAndCitationEqual(citation, target, status);
			if(status.getIsMatched()==1 ){
				status.setTargetCitationId(target.getCitationId());
				statusList.add(status);
				if(!inWindow)
					inWindow = true;
			}else if(inWindow){//out of window, impossible to find new matched ones.
				inWindow = false;
				break;
			}
			if(index == GRACE_ZONE && !inWindow)
				break;
		}
		return statusList;
	}
	private void insertMappingStatus(MappingStatus status) throws SQLException{
		LOGGER.info("Inserting mapping status. citation_id="+status.getCitationId()+"\tisMatched="+status.getIsMatched());
		prepInsertMapping.setLong(1, status.getCitationId());
		prepInsertMapping.setLong(2, status.getTargetCitationId());
		prepInsertMapping.setInt(3, status.getTitleComp());
		prepInsertMapping.setInt(4, status.getAuthorsComp());
		prepInsertMapping.setInt(5, status.getJournalComp());
		prepInsertMapping.setInt(6, status.getIsMatched());
		prepInsertMapping.setDate(7, new java.sql.Date(new java.util.Date().getTime()));
		prepInsertMapping.executeUpdate();
	}
	
	private boolean hasCitationChecked(long citationId) throws SQLException{
		prepSelectMapping.setLong(1, citationId);
		ResultSet rs = prepSelectMapping.executeQuery();
		boolean checked = rs.next();
		rs.close();
		return checked;
	}
	
	private boolean hasPair(long citationId, long targetCitationId) throws SQLException{
		prepSelectPair.setLong(1, targetCitationId);
		prepSelectPair.setLong(2, citationId);
		ResultSet rs = prepSelectPair.executeQuery();
		boolean has = rs.next();
		rs.close();
		return has;
	}
	
	public void closeAllStuff() throws SQLException, IOException{
		if(prepInsertMapping !=null)
			prepInsertMapping.close();
		if(prepSelectMapping !=null)
			prepSelectMapping.close();
		if(databaseConnection !=null)
			databaseConnection.close();
		if(searcher !=null)
			searcher.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			CitationMerger merger = new CitationMerger("/home/qzhang/elsevier_citation_index2");
			CitationService citationService = new CitationService();
			Citation citation = citationService.getCitationByCitationId(Long.parseLong(args[0]));
			List<CitationCitationMappingStatus> list = merger.mergeCitationWithoutDB(citation);
			FileWriter writer = new FileWriter(args[1]+args[0]+".txt");
			for(CitationCitationMappingStatus status: list){
				writer.append(status.toString()).append('\n');
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
