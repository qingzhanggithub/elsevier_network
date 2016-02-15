package edu.uwm.elsevier.namedisambiguation;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import articlesdata.article.Author;
import articlesdata.database.inputformat.DBWritable;

public class AuthorityEntity extends Author implements DBWritable{
	private String authorityId;
	private long pmid;
	private List<String> firstNameVariations;
	private List<String> lastNameVariations;
	private int isOriginal = 1;
	public String getAuthorityId() {
		return authorityId;
	}
	public void setAuthorityId(String authorityId) {
		this.authorityId = authorityId;
	}
	public long getPmid() {
		return pmid;
	}
	public void setPmid(long pmid) {
		this.pmid = pmid;
	}
	public List<String> getFirstNameVariations() {
		return firstNameVariations;
	}
	public void setFirstNameVariations(List<String> firstNameVariations) {
		this.firstNameVariations = firstNameVariations;
	}
	public List<String> getLastNameVariations() {
		return lastNameVariations;
	}
	public void setLastNameVariations(List<String> lastNameVariations) {
		this.lastNameVariations = lastNameVariations;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(authorityId).append('\t');
		sb.append(authorId).append('\t');
		sb.append(pmid).append('\t');
		return sb.toString();
	}
	public int getIsOriginal() {
		return isOriginal;
	}
	public void setIsOriginal(int isOriginal) {
		this.isOriginal = isOriginal;
	}
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		
	}
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		authorityId = rs.getString(1);
	}
	
	
}
