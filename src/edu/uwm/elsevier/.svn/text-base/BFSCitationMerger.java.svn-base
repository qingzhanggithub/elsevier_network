/**
 * 
 */
package edu.uwm.elsevier;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class BFSCitationMerger {
	
	private CitationNetworkService citationNetworkService;
	private ArticlesDataDBConnection databaseConnection;
	private static Logger LOGGER = NetworkBuilderLogger.getLogger("CitationMerger");
	private static String HAS_PROCESSED = "select citation_id from "+ITableNames.MERGED_CITATION_TABLE+" where citation_id=?";
	private PreparedStatement prepHasProc;
	
	public BFSCitationMerger() throws ClassNotFoundException, SQLException{
		citationNetworkService = new CitationNetworkService();
		databaseConnection = citationNetworkService.getCurrentConnection();
		prepHasProc = databaseConnection.getConnection().prepareStatement(HAS_PROCESSED);
	}
	
	public void mergeByBFS(long root) throws SQLException{
		Queue queue = new Queue();
		queue.enqueue(root);
		while(!queue.isEmpty()){
			long citationId = queue.dequeue();
			List<Long> targets = citationNetworkService.getUnmertedTargetCitationIds(root, citationId);
			citationNetworkService.insertMergedCitation(root, targets);
			queue.enqueueAll(targets);
		}
	}
	
	
	
	public void processMergingTable(int start) throws SQLException{
		LOGGER.debug("Started process merging table...");
		int max = (int)citationNetworkService.getMaxCitationId();
		int lower =start;
		int page = 1000;
		int upper = lower +page;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = null;
		while(lower < max){
			LOGGER.info("lower="+ lower);
			String cond = "citation_id >= "+lower+" and citation_id< "+ upper;
			String sql = "select citation_id from "+ITableNames.CITATION_MERGING_TABLE
					+" where "+cond;
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				if(!hasProcessed(rs.getLong(1)))
					mergeByBFS(rs.getLong(1));
			}
			lower = upper;
			upper += page;
		}
		rs.close();
		stmt.close();
		LOGGER.debug("Finished creating merged table.");
	}
	
	public boolean hasProcessed(long citationId) throws SQLException{
		prepHasProc.setLong(1, citationId);
		ResultSet rs = prepHasProc.executeQuery();
		boolean hasProcessed = rs.next();
		rs.close();
		return hasProcessed;
	}
	
	class Queue{
		private List<Long> list;
		public Queue(){
			list = new ArrayList<Long>();
		}
		public void enqueue(long ele){
			list.add(ele);
		}
		
		public void enqueueAll(List<Long> elems){
			for(Long ele: elems)
				list.add(ele);
		}
		public Long dequeue(){
			if(list.size() !=0){
				return list.remove(0);
			}else
				return null;
		}
		public boolean isEmpty(){
			return list.size()==0;
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length!=1){
			System.out.println("--start");
			return;
		}
		try {
			BFSCitationMerger merger = new BFSCitationMerger();
			merger.processMergingTable(Integer.parseInt(args[0]));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

}
