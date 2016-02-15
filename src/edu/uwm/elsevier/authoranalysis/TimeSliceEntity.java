package edu.uwm.elsevier.authoranalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class TimeSliceEntity implements Writable{
	private String authorityId;
	private int year;
	private int yearOffset;
	private int mincite;
	private int eincite;
	private int coAuthors;
	private float precentageInsititution;
	private int numPublications;

	@Override
	public void readFields(DataInput d) throws IOException {
		authorityId = WritableUtils.readString(d);
		year = d.readInt();
		yearOffset = d.readInt();
		mincite = d.readInt();
		eincite = d.readInt();
		coAuthors = d.readInt();
		precentageInsititution = d.readFloat();
		numPublications = d.readInt();
	}

	@Override
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, authorityId);
		d.writeInt(year);
		d.writeInt(yearOffset);
		d.writeInt(mincite);
		d.writeInt(eincite);
		d.writeInt(coAuthors);
		d.writeFloat(precentageInsititution);
		d.writeInt(numPublications);
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(authorityId).append('\t');
		sb.append(year).append('\t');
		sb.append(yearOffset).append('\t');
		sb.append(mincite).append('\t');
		sb.append(eincite).append('\t');
		sb.append(coAuthors).append('\t');
		sb.append(precentageInsititution).append('\t');
		sb.append(numPublications);
		return sb.toString();
	}

	
	public int getNumPublications() {
		return numPublications;
	}

	public void setNumPublications(int numPublications) {
		this.numPublications = numPublications;
	}

	public int getEincite() {
		return eincite;
	}

	public void setEincite(int eincite) {
		this.eincite = eincite;
	}

	public String getAuthorityId() {
		return authorityId;
	}

	public void setAuthorityId(String authorityId) {
		this.authorityId = authorityId;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMincite() {
		return mincite;
	}

	public void setMincite(int mincite) {
		this.mincite = mincite;
	}

	public int getCoAuthors() {
		return coAuthors;
	}

	public void setCoAuthors(int coAuthors) {
		this.coAuthors = coAuthors;
	}

	public float getPrecentageInsititution() {
		return precentageInsititution;
	}

	public void setPrecentageInsititution(float precentageInsititution) {
		this.precentageInsititution = precentageInsititution;
	}

	public int getYearOffset() {
		return yearOffset;
	}

	public void setYearOffset(int yearOffset) {
		this.yearOffset = yearOffset;
	}

}
