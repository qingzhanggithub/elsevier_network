/**
 * 
 */
package edu.uwm.elsevier.analysis;

/**
 * @author qing
 *
 */
public class Counter {
	
	protected int tp =0;
	protected int tn =0;
	protected int fp =0;
	protected int fn = 0;
	
	public int addToTp(int n){
		tp += n;
		return tp;
	}
	
	public int addToTn(int n){
		tn += n;
		return tn;
	}
	
	public int addToFp(int n){
		fp += n;
		return fp;
	}
	
	public int addToFn(int n){
		fn+= n;
		return fn;
	}

	public int getTp() {
		return tp;
	}

	public void setTp(int tp) {
		this.tp = tp;
	}

	public int getTn() {
		return tn;
	}

	public void setTn(int tn) {
		this.tn = tn;
	}

	public int getFp() {
		return fp;
	}

	public void setFp(int fp) {
		this.fp = fp;
	}

	public int getFn() {
		return fn;
	}

	public void setFn(int fn) {
		this.fn = fn;
	}
	
	public float getPrec(){
		return tp *1.0f / (tp+ fp);
	}
	
	public void addAll(Counter another){
		tp += another.tp;
		tn += another.tn;
		fp += another.fp;
		fn += another.fn;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("prec:").append(getPrec()).append('\t');
		sb.append("recall:").append(getRecall()).append('\t');
		return sb.toString();
	}
	
	public float getRecall(){
		return tp *1.0f / (tp+fn);
	}

}
