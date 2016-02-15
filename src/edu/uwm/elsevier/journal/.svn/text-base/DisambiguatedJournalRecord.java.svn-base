/**
 * 
 */
package edu.uwm.elsevier.journal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author qing
 *
 */
public class DisambiguatedJournalRecord extends JournalRecord implements Writable{

	private int cls;
	
	public DisambiguatedJournalRecord() {
	}

	/**
	 * @param articleId
	 * @param journal
	 * @param initialKey
	 */
	public DisambiguatedJournalRecord(int articleId, String journal,
			String initialKey) {
		super(articleId, journal, initialKey);
	}

	public int getCls() {
		return cls;
	}

	public void setCls(int cls) {
		this.cls = cls;
	}

	@Override
	public void readFields(DataInput d) throws IOException {
		journal = WritableUtils.readString(d);
		initialKey =WritableUtils.readString(d);
		cls = d.readInt();
	}

	@Override
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, journal);
		WritableUtils.writeString(d, initialKey);
		d.writeInt(cls);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(journal).append('\t');
		sb.append(initialKey).append('\t');
		sb.append(cls);
		return sb.toString();
	}
	
	
}
