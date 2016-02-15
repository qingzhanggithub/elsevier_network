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
public class CitationCitationMappingStatus extends MappingStatus implements Writable{
	private long citationId;
	private long targetCitationId;
	
	public CitationCitationMappingStatus(long citationId){
		this.citationId = citationId;
	}
	
	public CitationCitationMappingStatus(){
		
	}
	
	public long getCitationId() {
		return citationId;
	}
	public void setCitationId(long citationId) {
		this.citationId = citationId;
	}
	public long getTargetCitationId() {
		return targetCitationId;
	}
	public void setTargetCitationId(long targetCitationId) {
		this.targetCitationId = targetCitationId;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(citationId).append('\t');
		sb.append(targetCitationId).append('\t');
		sb.append(titleComp).append('\t');
		sb.append(authorsComp).append('\t');
		sb.append(journalComp).append('\t');
		sb.append(is_matched);
		return sb.toString();
	}

	@Override
	public void readFields(DataInput d) throws IOException {
		citationId = d.readLong();
		targetCitationId  = d.readLong();
		titleComp = d.readInt();
		authorsComp = d.readInt();
		journalComp = d.readInt();
		is_matched = d.readInt();
	}

	@Override
	public void write(DataOutput d) throws IOException {
		d.writeLong(citationId);
		d.writeLong(targetCitationId);
		d.writeInt(titleComp);
		d.writeInt(authorsComp);
		d.writeInt(journalComp);
		d.writeInt(is_matched);
	}
}
