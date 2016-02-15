/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author qing
 *
 */
public class AuthorshipEdge extends Edge implements WritableComparable<AuthorshipEdge>{

	protected List<PublicationAttribute> publications = new ArrayList<PublicationAttribute>();
	
	public AuthorshipEdge(){
		
	}
	
	public AuthorshipEdge(String src, String dest) {
		super(src, dest);
	}

	public List<PublicationAttribute> getPublications() {
		return publications;
	}

	@Override
	public void readFields(DataInput d) throws IOException {
		src = WritableUtils.readString(d);
		dest = WritableUtils.readString(d);
	}

	@Override
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, src);
		WritableUtils.writeString(d, dest);
	}

	@Override
	public int compareTo(AuthorshipEdge other) {
		
		int comp = this.src.compareTo(this.dest);
		String total = null;
		if(comp == 1){
			total = this.dest+" "+this.src;
		}else
			total = this.src+" "+this.dest;
		
		int othercomp = other.src.compareTo(other.dest);
		String otherTotal = null;
		if(othercomp ==1)
			otherTotal = other.dest+" "+other.src;
		else
			otherTotal = other.src+" "+other.dest;
		
		
		return total.compareTo(otherTotal);
	}

}
