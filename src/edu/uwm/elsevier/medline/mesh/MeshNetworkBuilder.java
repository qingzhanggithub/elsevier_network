/**
 * 
 */
package edu.uwm.elsevier.medline.mesh;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import pmidmapper.MedlineSearcher;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.medline.CitationNetworkEdge;
import edu.uwm.elsevier.medline.MedlineAritcleNodeStatistiscs;

/**
 * @author qing
 *
 */
public class MeshNetworkBuilder {
	
	private CitationNetworkService citationNetworkService;
	private static Logger LOGGER = Logger.getLogger("MeshNetworkBuilder");

	
	public MeshNetworkBuilder() throws ClassNotFoundException, SQLException{
		citationNetworkService = new CitationNetworkService();
	}
	
	public List<MeshNetworkEdge> buildMeshCitationLink(CitationNetworkEdge edge) throws SQLException{
		MedlineAritcleNodeStatistiscs src = 
				citationNetworkService.getMedlineAritcleNodeStatistiscsByArticleId(edge.getSrcArticleId());
		MedlineAritcleNodeStatistiscs dest = 
				citationNetworkService.getMedlineAritcleNodeStatistiscsByArticleId(edge.getDestArticleId());
		List<String> srcMeshs = MedlineSearcher.parseMeshs(src.getMeshs());
		List<String> destMeshs = MedlineSearcher.parseMeshs(dest.getMeshs());
		Set<Integer> srcMeshIds = new HashSet<Integer>();
		Set<Integer> destMeshIds = new HashSet<Integer>();
		for(String mesh: srcMeshs){
			int meshId = citationNetworkService.getMeshIdByMesh(mesh);
			if(meshId != -1)
				srcMeshIds.add(meshId);
			else
				LOGGER.error("No such mesh term: "+mesh);
		}
		for(String mesh: destMeshs){
			int meshId = citationNetworkService.getMeshIdByMesh(mesh);
			if(meshId != -1)
				destMeshIds.add(meshId);
			else
				LOGGER.error("No such mesh term: "+mesh);
		}
		List<MeshNetworkEdge> meshEdges = new ArrayList<MeshNetworkEdge>();
		for(int srcMeshId: srcMeshIds){
			for(int destMeshId: destMeshIds){
				MeshNetworkEdge meshEdge = new MeshNetworkEdge(srcMeshId, destMeshId);
				meshEdges.add(meshEdge);
			}
		}
		return meshEdges;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
