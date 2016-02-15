/**
 * 
 */
package edu.uwm.elsevier.prediction;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.mysql.jdbc.Util;

import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;
import edu.uwm.elsevier.utils.IOUtils;

/**
 * @author qing
 *
 */
public class LinkPredictionBaseline {
	
	private int yearStart;
	private Set<String> taoX =null;
	private Set<String> taoY = null;
	private String authorityX;
	private String authorityY;
	
	private AuthorDSBService authorDSBService ;
	public LinkPredictionBaseline(int yearStart) throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
		this.yearStart = yearStart;
	}
	
	public void initTao(String authorityX, String authorityY) throws SQLException{
		this.authorityX = authorityX;
		this.authorityY = authorityY;
		taoX = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityX, -1, yearStart));
		taoY = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityY, -1, yearStart));
	}

	public float getJaccardCoef() throws SQLException{
		int inter = intersection(taoX, taoY).size();
		int union = union(taoX, taoY).size();
		float coef = 0;
		if(union!=0)
			coef = 1.0f * inter/union;
		System.out.println("Jaccard. inter:"+inter+"\tunion:"+union+"\tcoef:"+coef);
		return coef;
	}
	
	public float getAdamicAdar() throws SQLException{
		System.out.println("Adamic:");
		Set<String> setZ = intersection(taoX, taoY);
		System.out.println("setZ.size: "+setZ.size());
		float sum = 0f;
		for(String authorityZ: setZ){
			HashSet<String> taoZ = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityZ, -1, yearStart));
			if(taoZ.size()==1){
				System.err.println("The neighbor "+authorityZ+" of ("+authorityX+", "+authorityY+") is 1.");
			}
			System.out.print(taoZ.size()+"\t");
			sum += 1.0f/Math.log(taoZ.size());
		}
		System.out.println("value: "+sum);
		return sum;
	}
	
	public float getPreferencialAttachment(){
		return 1.0f * taoX.size() * taoY.size();
	}
	
	public static Set<String> intersection(Set<String> setX, Set<String> setY){
		Set<String> set = new HashSet<String>();
		for(String s: setX){
			if(setY.contains(s))
				set.add(s);
		}
		return set;
	}
	
	public static Set<String> union(Set<String> setX, Set<String> setY){
		Set<String> set = new HashSet<String>();
		set.addAll(setX);
		set.addAll(setY);
		return set;
	}
	
	public void processPairs(String path) throws IOException, SQLException{
		System.out.println("Getting baseline for "+path);
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		FileWriter writer = new FileWriter(path+".baseline");
		String strLine;
		while ((strLine = br.readLine()) != null){
			String[] pairs = strLine.split(",");
			writer.append(pairs[0]).append(',').append(pairs[1]).append(',');
			initTao(pairs[0], pairs[1]);
			System.out.println("---- "+pairs[0]+", "+pairs[1]+" ---");
			float jaccard = getJaccardCoef();
			float adamic = getAdamicAdar();
			float pref = getPreferencialAttachment();
			writer.append(String.valueOf(jaccard)).append(',').append(String.valueOf(adamic)).append(',').append(String.valueOf(pref)).append('\n');
		}
		writer.close();
		br.close();
		in.close();
		fstream.close();
		System.out.println("Task done.");
	}
	
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws NumberFormatException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws NumberFormatException, ClassNotFoundException, SQLException, IOException {
		if(args.length != 3){
			System.out.println("--pair-path --year-start --year-end");
			return;
		}
		LinkPredictionBaseline baseline = new LinkPredictionBaseline(Integer.parseInt(args[1]));
		baseline.processPairs(args[0]);
	}

}
