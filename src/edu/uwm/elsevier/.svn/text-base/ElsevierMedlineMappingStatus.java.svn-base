/**
 * 
 */
package edu.uwm.elsevier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author qing
 *
 */
public class ElsevierMedlineMappingStatus extends MappingStatus implements Writable{
	
	private int articleId;
	private long pmid;
	
	public ElsevierMedlineMappingStatus(){
		
	}
	
	public ElsevierMedlineMappingStatus(long pmid, int articleId){
		this.pmid = pmid;
		this.articleId = articleId;
	}
	public int getArticleId() {
		return articleId;
	}
	public void setArticleId(int articleId) {
		this.articleId = articleId;
	}
	public long getPmid() {
		return pmid;
	}
	public void setPmid(long pmid) {
		this.pmid = pmid;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(articleId).append('\t');
		sb.append(pmid).append('\t');
		sb.append(titleComp).append('\t');
		sb.append(authorsComp).append('\t');
		sb.append(journalComp).append('\t');
		sb.append(is_matched);
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput d) throws IOException {
		articleId = d.readInt();
		pmid  = d.readLong();
		titleComp = d.readInt();
		authorsComp = d.readInt();
		journalComp = d.readInt();
		is_matched = d.readInt();
	}

	@Override
	public void write(DataOutput d) throws IOException {
		d.writeInt(articleId);
		d.writeLong(pmid);
		d.writeInt(titleComp);
		d.writeInt(authorsComp);
		d.writeInt(journalComp);
		d.writeInt(is_matched);
	}

}
