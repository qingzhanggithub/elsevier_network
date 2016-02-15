/**
 * 
 */
package edu.uwm.elsevier.journal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import edu.uwm.elsevier.CitationArticleComparison;

/**
 * @author qing
 *
 */
public class JournalRecord implements Writable{
	protected int articleId;
	protected String journal;
	protected String initialKey;
	
	public JournalRecord(){
		
	}
	
	public JournalRecord(int articleId, String journal, String initialKey){
		this.articleId = articleId;
		this.journal = journal;
		this.initialKey = initialKey;
	}
	
	@Override
	public void readFields(DataInput d) throws IOException {
		articleId = d.readInt();
		journal = WritableUtils.readString(d);
		initialKey = WritableUtils.readString(d);
	}

	@Override
	public void write(DataOutput d) throws IOException {
		d.writeInt(articleId);
		WritableUtils.writeString(d, journal);
		WritableUtils.writeString(d, initialKey);
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(initialKey).append('\t');
		sb.append(journal).append('\t');
		sb.append(articleId);
		return sb.toString();
	}

	public int getArticleId() {
		return articleId;
	}

	public void setArticleId(int articleId) {
		this.articleId = articleId;
	}

	public String getJournal() {
		return journal;
	}

	public void setJournal(String journal) {
		this.journal = journal;
	}

	public String getInitialKey() {
		return initialKey;
	}

	public void setInitialKey(String initialKey) {
		this.initialKey = initialKey;
	}

	public boolean equalsByName(JournalRecord other){
		List<String> srcTokens = CitationArticleComparison.tokenizationAndRemoveStopWords(journal, JournalNameDsb.JOURNAL_TOKENIZATION_REGEX);
		List<String> targetTokens = CitationArticleComparison.tokenizationAndRemoveStopWords(other.journal, JournalNameDsb.JOURNAL_TOKENIZATION_REGEX);
		for(String src: srcTokens){
			boolean matched = false;
			for(String target: targetTokens){
				if(src.startsWith(target) || target.startsWith(src)){
					matched = true;
					break;
				}
			}
			if(!matched)
				return false;
		}
		return true;
	}
	
}
