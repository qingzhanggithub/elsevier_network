/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import articlesdata.database.inputformat.DBWritable;

/**
 * @author qing
 *
 */
public class MEDLINEArticleTrendingEntity extends ArticleNodeStatistics implements Writable,DBWritable {
	protected String minciteYears;
	protected String einciteYears;
	public String getMinciteYears() {
		return minciteYears;
	}
	public void setMinciteYears(String minciteYears) {
		this.minciteYears = minciteYears;
	}
	public String getEinciteYears() {
		return einciteYears;
	}
	public void setEinciteYears(String einciteYears) {
		this.einciteYears = einciteYears;
	}
	@Override
	public void readFields(DataInput d) throws IOException {
		articleId = d.readInt();
		minciteYears = WritableUtils.readString(d);
		einciteYears = WritableUtils.readString(d);
	}
	@Override
	public void write(DataOutput d) throws IOException {
		d.writeInt(articleId);
		WritableUtils.writeString(d, minciteYears);
		WritableUtils.writeString(d, einciteYears);
	}
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(articleId).append('\t');
		sb.append(minciteYears).append('\t');
		sb.append(einciteYears);
		return sb.toString();
	}
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		articleId = rs.getInt("article_id");
	}
	
}
