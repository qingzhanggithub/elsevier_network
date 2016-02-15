/**
 * 
 */
package edu.uwm.elsevier.prediction.hadoop;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import articlesdata.database.inputformat.DBWritable;

/**
 * @author qing
 *
 */
public class CollabPair implements DBWritable {
	private String src;
	private String dest;
	private int collabTimes;
	private int startYear;
	private int endYear;

	
	
	public String getSrc() {
		return src;
	}


	public void setSrc(String src) {
		this.src = src;
	}


	public String getDest() {
		return dest;
	}


	public void setDest(String dest) {
		this.dest = dest;
	}


	public int getCollabTimes() {
		return collabTimes;
	}


	public void setCollabTimes(int collabTimes) {
		this.collabTimes = collabTimes;
	}


	public int getStartYear() {
		return startYear;
	}


	public void setStartYear(int startYear) {
		this.startYear = startYear;
	}


	public int getEndYear() {
		return endYear;
	}


	public void setEndYear(int endYear) {
		this.endYear = endYear;
	}


	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub

	}

	
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		src = rs.getString("src");
		dest = rs.getString("dest");
		collabTimes = rs.getInt("collab_times");
		startYear = rs.getInt("start_year");
		endYear = rs.getInt("end_year");
	}

}
