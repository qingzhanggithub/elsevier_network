/**
 * 
 */
package edu.uwm.elsevier.medline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import edu.uwm.elsevier.analysis.ArticleNodeStatistics;

/**
 * @author qing
 *
 */
public class MedlineAritcleNodeStatistiscs extends ArticleNodeStatistics implements Writable{

	private long pmid;
	private String meshs = null;
	private int year=0;
	private int incitesFromMedline =0;
	private int outcitesToMedline=0;
	private String journal = null;
	
	public MedlineAritcleNodeStatistiscs(){
		
	}
	
	public MedlineAritcleNodeStatistiscs(int articleId, long pmid){
		this.articleId = articleId;
		this.pmid = pmid;
	}
	public long getPmid() {
		return pmid;
	}
	public void setPmid(long pmid) {
		this.pmid = pmid;
	}
	public String getMeshs() {
		return meshs;
	}
	public void setMeshs(String meshs) {
		this.meshs = meshs;
	}
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getIncitesFromMedline() {
		return incitesFromMedline;
	}
	public void setIncitesFromMedline(int incitesFromMedline) {
		this.incitesFromMedline = incitesFromMedline;
	}
	public int getOutcitesToMedline() {
		return outcitesToMedline;
	}
	public void setOutcitesToMedline(int outcitesToMedline) {
		this.outcitesToMedline = outcitesToMedline;
	}
	public String getJournal() {
		return journal;
	}
	public void setJournal(String journal) {
		this.journal = journal;
	}
	@Override
	public void readFields(DataInput d) throws IOException {

		articleId = d.readInt();
		pmid = d.readLong();
		incites = d.readInt();
		incitesFromMedline = d.readInt();
		outcites = d.readInt();
		outcitesToMedline = d.readInt();
		numOfAuthors = d.readInt();
		year = d.readInt();
		journal = WritableUtils.readString(d);
		meshs = WritableUtils.readString(d);
	}
	@Override
	public void write(DataOutput d) throws IOException {
		d.writeInt(articleId);
		d.writeLong(pmid);
		d.writeInt(incites);
		d.writeInt(incitesFromMedline);
		d.writeInt(outcites);
		d.writeInt(outcitesToMedline);
		d.writeInt(numOfAuthors);
		d.writeInt(year);
		WritableUtils.writeString(d, journal==null?"NA":journal);
		WritableUtils.writeString(d, meshs==null?"NA":meshs);
		
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(articleId).append('\t');
		sb.append(pmid).append('\t');
		sb.append(incites).append('\t');
		sb.append(incitesFromMedline).append('\t');
		sb.append(outcites).append('\t');
		sb.append(outcitesToMedline).append('\t');
		sb.append(numOfAuthors).append('\t');
		sb.append(year).append('\t');
		sb.append(journal).append('\t');
		sb.append(meshs);
		return sb.toString();
	}
	
	
}
