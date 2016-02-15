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
public class MeshEntity implements Writable,DBWritable {
	protected String mesh;
	protected int meshId;
	protected String articleIds;
	protected int minciteCount;
	protected int einciteCount;
	protected int idSetSize;
	protected String minciteYears;
	protected String einciteYears;
	
	public String getEinciteYears() {
		return einciteYears;
	}
	public void setEinciteYears(String einciteYears) {
		this.einciteYears = einciteYears;
	}
	public String getMinciteYears() {
		return minciteYears;
	}
	public void setMinciteYears(String minciteYears) {
		this.minciteYears = minciteYears;
	}
	
	public int getMeshId() {
		return meshId;
	}
	public void setMeshId(int meshId) {
		this.meshId = meshId;
	}
	public String getArticleIds() {
		return articleIds;
	}
	public void setArticleIds(String articleIds) {
		this.articleIds = articleIds;
	}
	public String getMesh() {
		return mesh;
	}
	public void setMesh(String mesh) {
		this.mesh = mesh;
	}
	public int getMinciteCount() {
		return minciteCount;
	}
	public void setMinciteCount(int minciteCount) {
		this.minciteCount = minciteCount;
	}
	public int getEinciteCount() {
		return einciteCount;
	}
	public void setEinciteCount(int einciteCount) {
		this.einciteCount = einciteCount;
	}
	public int getIdSetSize() {
		return idSetSize;
	}
	public void setIdSetSize(int idSetSize) {
		this.idSetSize = idSetSize;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(mesh).append('\t');
		sb.append(minciteCount).append('\t');
		sb.append(einciteCount).append('\t');
		sb.append(idSetSize).append('\t');
		sb.append(minciteCount*1.0f/idSetSize).append('\t');
		sb.append(einciteCount*1.0f/idSetSize).append('\t');
		sb.append(minciteYears);
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput d) throws IOException {
		mesh = WritableUtils.readString(d);
		minciteCount = d.readInt();
		einciteCount = d.readInt();
		idSetSize = d.readInt();
		minciteYears = WritableUtils.readString(d);
//		einciteYears = WritableUtils.readString(d);// many years are missing. don't do it now.
	}
	@Override
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, mesh);
		d.writeInt(minciteCount);
		d.writeInt(einciteCount);
		d.writeInt(idSetSize);
		WritableUtils.writeString(d, minciteYears);
//		WritableUtils.writeString(d, einciteYears); // many years are missing. don't do it now.
	}
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		
	}
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		meshId = rs.getInt("mesh_id");
		mesh = rs.getString("mesh");
		articleIds = rs.getString("article_ids");
	}
	
	
}
