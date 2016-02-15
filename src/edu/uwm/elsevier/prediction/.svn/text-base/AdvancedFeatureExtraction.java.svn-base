package edu.uwm.elsevier.prediction;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;

public class AdvancedFeatureExtraction {

	
	private int endYear;
	private int startYear =-1;
	private Set<String> taoX =null;
	private Set<String> taoY = null;
	private String authorityX;
	private String authorityY;
	private AuthorDSBService authorDSBService ;
	
	
	public AdvancedFeatureExtraction() throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
	}
	/**
	 * Checked code.
	 * @param authorityX
	 * @param authorityY
	 * @param year
	 * @throws SQLException
	 */
	public void reset(String authorityX, String authorityY, int year) throws SQLException{
		this.authorityX = authorityX;
		this.authorityY = authorityY;
		taoX = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityX, -1, year));
		taoY = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityY, -1, year));
		this.endYear = year;
	}
	
	public void reset(String authorityX, String authorityY, int startYear,  int endYear) throws SQLException{
		this.authorityX = authorityX;
		this.authorityY = authorityY;
		taoX = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityX, startYear, endYear));
		taoY = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityY, startYear, endYear));
		this.startYear = startYear;
		this.endYear = endYear;
	}
	
	public float getJaccardCoef() throws SQLException{
		int inter = LinkPredictionBaseline.intersection(taoX, taoY).size();
		int union = LinkPredictionBaseline.union(taoX, taoY).size();
		float coef = 0;
		if(union!=0)
			coef = 1.0f * inter/union;
//		System.out.println("Jaccard. inter:"+inter+"\tunion:"+union+"\tcoef:"+coef);
		return coef;
	}
	/**
	 * Checked code
	 * @return
	 * @throws SQLException
	 */
	public float getAdamicAdar() throws SQLException{
//		System.out.println("Adamic:");
		Set<String> setZ = LinkPredictionBaseline.intersection(taoX, taoY);
//		System.out.println("setZ.size: "+setZ.size());
		float sum = 0.0f;
		for(String authorityZ: setZ){
			HashSet<String> taoZ = new HashSet<String>(authorDSBService.getCoAuthorsByAuthorityIdBetweenYears(authorityZ, startYear, endYear));
			if(taoZ.size()==1){
				System.err.println("The neighbor "+authorityZ+" of ("+authorityX+", "+authorityY+") is 1. Impossible. it is at least 2.");
			}
//			System.out.print(taoZ.size()+"\t");
			sum += 1.0f/Math.log(taoZ.size());
		}
//		System.out.println("value: "+sum);
		return sum;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
