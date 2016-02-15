/**
 * 
 */
package edu.uwm.elsevier.prediction;

/**
 * @author qing
 *
 */
public class Edge {
	
	protected String src;
	protected String dest;
	
	public Edge(){
		
	}
	
	
	public Edge(String src, String dest){
		this.src = src;
		this.dest = dest;
	}

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

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof Edge))
			return false;
		Edge other = (Edge)o;
		if(src.equalsIgnoreCase(other.src) && dest.equalsIgnoreCase(other.dest) || 
				src.equalsIgnoreCase(other.dest) && dest.equalsIgnoreCase(src))
			return true;
		return false;
	}

}
