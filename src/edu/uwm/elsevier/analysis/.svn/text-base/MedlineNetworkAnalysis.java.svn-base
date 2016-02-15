/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.sql.SQLException;

import pmidmapper.PMArticle;


/**
 * @author qing
 *
 */
public class MedlineNetworkAnalysis {

	
	public MedlineNetworkAnalysis() throws ClassNotFoundException, SQLException{
	}
	public MEDLINEArticleTrendingEntity getCiationYearTrending(PMArticle article) throws SQLException, ClassNotFoundException{
		MEDLINEArticleTrendingEntity entity = new MEDLINEArticleTrendingEntity();
		entity.setArticleId(article.getArticleId());
		String einciteYears = MeshAnalysis.getEinciteYears(entity.getArticleId());
		String minciteYears = MeshAnalysis.getMinciteYears(entity.getArticleId());
		entity.setEinciteYears(einciteYears);
		entity.setMinciteYears(minciteYears);
		return entity;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
