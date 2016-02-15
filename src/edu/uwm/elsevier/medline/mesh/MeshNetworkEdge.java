/**
 * 
 */
package edu.uwm.elsevier.medline.mesh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author qing
 *
 */
public class MeshNetworkEdge implements WritableComparable<MeshNetworkEdge>{
	
	protected int srcMeshId;
	protected int destMeshId;
	protected int count=0;
	
	public MeshNetworkEdge(int srcMeshId, int destMeshId){
		this.srcMeshId = srcMeshId;
		this.destMeshId = destMeshId;
	}
	
	public MeshNetworkEdge(){
		
	}

	public int getSrcMeshId() {
		return srcMeshId;
	}

	public void setSrcMeshId(int srcMeshId) {
		this.srcMeshId = srcMeshId;
	}

	public int getDestMeshId() {
		return destMeshId;
	}

	public void setDestMeshId(int destMeshId) {
		this.destMeshId = destMeshId;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput d) throws IOException {
		srcMeshId = d.readInt();
		destMeshId = d.readInt();
	}

	@Override
	public void write(DataOutput d) throws IOException {
		d.writeInt(srcMeshId);
		d.writeInt(destMeshId);
	}
	
	@Override
	public String toString(){
		return srcMeshId+"\t"+destMeshId;
	}

	@Override
	public int compareTo(MeshNetworkEdge edge) {
		if(srcMeshId < edge.getSrcMeshId())
			return -1;
		else if(srcMeshId > edge.getSrcMeshId())
			return 1;
		else if(destMeshId < edge.getDestMeshId()) // srcs equal
			return -1;
		else if(destMeshId >  edge.getDestMeshId()) // srcs equal
			return 1;
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof MeshNetworkEdge))
			return false;
		MeshNetworkEdge edge = (MeshNetworkEdge)obj;
		if(edge.getSrcMeshId() == srcMeshId && edge.getDestMeshId() == destMeshId)
			return true;
		return false;
	}

	@Override
	public int hashCode() {
		return Float.floatToIntBits(srcMeshId)^Float.floatToIntBits(destMeshId);
	}
	
	

}
