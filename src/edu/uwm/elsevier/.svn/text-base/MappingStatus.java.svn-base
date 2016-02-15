/**
 * 
 */
package edu.uwm.elsevier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

/**
 * @author qing
 *
 */
public class MappingStatus {
	
	private long citationId;
	private int articleId;
	private long targetCitationId;
	private long pmid;
	protected int is_matched = -1; // 1 yes. 0 no. -1 not processed.
	protected Date date;
	protected int authorsComp = -1;
	protected int titleComp = -1;
	protected int journalComp = -1;
	protected int dateComp;
	
	public MappingStatus(){
		
	}
	
	
	public MappingStatus(long citationId,int articleId,int is_matched){
		this.citationId = citationId;
		this.articleId = articleId;
		this.is_matched = is_matched;
	}

	public MappingStatus(long citationId,int articleId){
		this.citationId = citationId;
		this.articleId = articleId;
	}
	
	public MappingStatus(long citationId){
		this.citationId = citationId;
	}
	
	public long getCitationId() {
		return citationId;
	}
	
	

	public long getTargetCitationId() {
		return targetCitationId;
	}

	public void setTargetCitationId(long targetCitationId) {
		this.targetCitationId = targetCitationId;
	}

	public void setCitationId(long citationId) {
		this.citationId = citationId;
	}

	public int getArticleId() {
		return articleId;
	}

	public void setArticleId(int articleId) {
		this.articleId = articleId;
	}

	public int getIsMatched() {
		return is_matched;
	}

	public void setIsMatched(int is_matched) {
		this.is_matched = is_matched;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public int getAuthorsComp() {
		return authorsComp;
	}

	public void setAuthorsComp(int authorsComp) {
		this.authorsComp = authorsComp;
	}

	public int getTitleComp() {
		return titleComp;
	}

	public void setTitleComp(int titleComp) {
		this.titleComp = titleComp;
	}

	public int getJournalComp() {
		return journalComp;
	}

	public void setJournalComp(int journalComp) {
		this.journalComp = journalComp;
	}

	public int getDateComp() {
		return dateComp;
	}

	public void setDateComp(int dateComp) {
		this.dateComp = dateComp;
	}
	
	
	
	public long getPmid() {
		return pmid;
	}


	public void setPmid(long pmid) {
		this.pmid = pmid;
	}


	public  boolean isEqual(){
		if(titleComp==CitationArticleComparison.EQUAL && authorsComp==CitationArticleComparison.EQUAL)
			return true;
		else if(authorsComp==CitationArticleComparison.EQUAL && journalComp==CitationArticleComparison.EQUAL)
			return true;
		else if(titleComp == CitationArticleComparison.EQUAL && journalComp== CitationArticleComparison.EQUAL)
			return true;
		else 
			return false;
	}
	
}
