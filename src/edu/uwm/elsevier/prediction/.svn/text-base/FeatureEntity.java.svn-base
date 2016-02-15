/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author qing
 *
 */
public class FeatureEntity implements Writable{
	
	protected String src;
	protected String dest;
	protected float sumPub;
	protected float simMesh;
	protected float simFulltex;
	protected int numCommonFriend;
	protected float sumCoAuthor;
	protected float sumClusteringCoef;
	protected float simIncite;
	protected float simOutcite;
	protected float recency;
	protected float positionSim;
	protected float instituteSimilarity;
	protected float jaccard;
	protected float adamic;
	protected String cls;
	
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
	public float getSumPub() {
		return sumPub;
	}
	public void setSumPub(float sumPub) {
		this.sumPub = sumPub;
	}
	public float getSimMesh() {
		return simMesh;
	}
	public void setSimMesh(float simMesh) {
		this.simMesh = simMesh;
	}
	public float getSimFulltex() {
		return simFulltex;
	}
	public void setSimFulltex(float simFulltex) {
		this.simFulltex = simFulltex;
	}
	public int getSumCommonFriend() {
		return numCommonFriend;
	}
	public void setSumCommonFriend(int sumCommonFriend) {
		this.numCommonFriend = sumCommonFriend;
	}
	public float getSumCoAuthor() {
		return sumCoAuthor;
	}
	public void setSumCoAuthor(float sumCoAuthor) {
		this.sumCoAuthor = sumCoAuthor;
	}
	public float getSumClusteringCoef() {
		return sumClusteringCoef;
	}
	public void setSumClusteringCoef(float sumClusteringCoef) {
		this.sumClusteringCoef = sumClusteringCoef;
	}
	public float getSimIncite() {
		return simIncite;
	}
	public void setSimIncite(float simIncite) {
		this.simIncite = simIncite;
	}
	public float getSimOutcite() {
		return simOutcite;
	}
	public void setSimOutcite(float simOutcite) {
		this.simOutcite = simOutcite;
	}
	public float getRecency() {
		return recency;
	}
	public void setRecency(float recency) {
		this.recency = recency;
	}
	public float getPositionDiff() {
		return positionSim;
	}
	public void setPositionDiff(float positionDiff) {
		this.positionSim = positionDiff;
	}
	public float getInstituteSimilarity() {
		return instituteSimilarity;
	}
	public void setInstituteSimilarity(float instituteSimilarity) {
		this.instituteSimilarity = instituteSimilarity;
	}
	
	public int getNumCommonFriend() {
		return numCommonFriend;
	}
	public void setNumCommonFriend(int numCommonFriend) {
		this.numCommonFriend = numCommonFriend;
	}
	public float getPositionSim() {
		return positionSim;
	}
	public void setPositionSim(float positionSim) {
		this.positionSim = positionSim;
	}
	public float getJaccard() {
		return jaccard;
	}
	public void setJaccard(float jaccard) {
		this.jaccard = jaccard;
	}
	public float getAdamic() {
		return adamic;
	}
	public void setAdamic(float adamic) {
		this.adamic = adamic;
	}
	
	
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		String fieldSep = ",";
		sb.append(src).append(fieldSep);
		sb.append(dest).append(fieldSep);
		sb.append(simFulltex).append(fieldSep);
		sb.append(simMesh).append(fieldSep);
		sb.append(simIncite).append(fieldSep);
		sb.append(simOutcite).append(fieldSep);
		sb.append(sumPub).append(fieldSep);
		sb.append(sumCoAuthor).append(fieldSep);
		sb.append(numCommonFriend).append(fieldSep);
		sb.append(sumClusteringCoef).append(fieldSep);
		sb.append(recency).append(fieldSep);
		sb.append(positionSim).append(fieldSep);
		sb.append(instituteSimilarity).append(fieldSep);
		sb.append(jaccard).append(fieldSep);
		sb.append(adamic);
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		
	}
}
