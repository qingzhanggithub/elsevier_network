/**
 * 
 */
package edu.uwm.elsevier.authoranalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author qing
 *
 */
public class ValueEntity implements Writable{
	
	private String authorityId;
	private float value;
	
	public String getAuthorityId() {
		return authorityId;
	}
	public void setAuthorityId(String authorityId) {
		this.authorityId = authorityId;
	}
	public float getValue() {
		return value;
	}
	public void setValue(float value) {
		this.value = value;
	}
	@Override
	public void readFields(DataInput d) throws IOException {
		authorityId = WritableUtils.readString(d);
		value = d.readFloat();
	}
	@Override
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, authorityId);
		d.writeFloat(value);
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(authorityId).append('\t');
		sb.append(value);
		return sb.toString();
	}

}
