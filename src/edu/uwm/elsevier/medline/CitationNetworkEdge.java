/**
 * 
 */
package edu.uwm.elsevier.medline;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import articlesdata.database.inputformat.DBWritable;

/**
 * @author qing
 *
 */
public class CitationNetworkEdge implements DBWritable {
	
	protected int srcArticleId;
	
	protected int destArticleId;
	
	public CitationNetworkEdge(int srcArticleId, int destArticleId){
		this.srcArticleId = srcArticleId;
		this.destArticleId = destArticleId;
	}
	
	public CitationNetworkEdge(){
		
	}

	public int getSrcArticleId() {
		return srcArticleId;
	}

	public void setSrcArticleId(int srcArticleId) {
		this.srcArticleId = srcArticleId;
	}

	public int getDestArticleId() {
		return destArticleId;
	}

	public void setDestArticleId(int destArticleId) {
		this.destArticleId = destArticleId;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		srcArticleId = rs.getInt("src_article_id");
		destArticleId = rs.getInt("dest_article_id");
	}
	
	

}
