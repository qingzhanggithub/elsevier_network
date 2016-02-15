/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.util.List;

/**
 * @author qing
 *
 */
public class AuthorVecInfo {
	
	private int authorId;
	private int articleId;
	private int pmid;
	private String lastName;
	private String firstName;
	private String middleName;
	private List<Integer> inciteArticleIds;
	private float[] tfidfs;
	private String wholeNameType =null;
	
	public AuthorVecInfo(int authorId){
		this.authorId = authorId;
	}

	public int getAuthorId() {
		return authorId;
	}

	public void setAuthorId(int authorId) {
		this.authorId = authorId;
	}

	public int getArticleId() {
		return articleId;
	}

	public void setArticleId(int articleId) {
		this.articleId = articleId;
	}

	public int getPmid() {
		return pmid;
	}

	public void setPmid(int pmid) {
		this.pmid = pmid;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public List<Integer> getInciteArticleIds() {
		return inciteArticleIds;
	}

	public void setInciteArticleIds(List<Integer> inciteArticleIds) {
		this.inciteArticleIds = inciteArticleIds;
	}

	public float[] getTfidfs() {
		return tfidfs;
	}

	public void setTfidfs(float[] tfidfs) {
		this.tfidfs = tfidfs;
	}

	public String getMiddleName() {
		return middleName;
	}

	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}
	
	public boolean isEqual(AuthorVecInfo another){
		boolean isLastEqual = false;
		if(lastName==null && another.getLastName()==null || lastName.equalsIgnoreCase(another.getLastName()))
			isLastEqual = true;
		boolean isFirstEqual = false;
		if(firstName == null && another.getFirstName()==null || firstName.equalsIgnoreCase(another.getFirstName()))
			isFirstEqual = true;
		boolean isMiddleEqual = false;
		if(middleName == null && another.getMiddleName()== null || middleName.equalsIgnoreCase(another.getMiddleName()))
			isMiddleEqual = true;
		return isLastEqual && isFirstEqual && isMiddleEqual;
	}
	
	public String getNameType(String name){
		String type ="2";
		if(name == null || name.length()==0)
			type = "2";
		else if(name.length() >1)
			type = "1";
		else if(name.length() == 1)
			type = "0";
		return type;
	}
	
	public String getWholeNameType(){
		if(wholeNameType == null){
			StringBuffer sb = new StringBuffer();
			sb.append(getNameType(lastName));
			sb.append(getNameType(firstName));
			sb.append(getNameType(middleName));
			wholeNameType = sb.toString();
		}
		return wholeNameType;
	}

}
