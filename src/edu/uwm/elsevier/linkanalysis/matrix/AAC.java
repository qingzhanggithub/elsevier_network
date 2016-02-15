package edu.uwm.elsevier.linkanalysis.matrix;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uwm.elsevier.CitationNetworkService;
import edu.uwm.elsevier.namedisambiguation.AuthorDSBService;

public class AAC {
	
	private AdjacentMatrix adjacentMatrix;
	private AuthorDSBService authorDSBService;
	private CitationNetworkService citationNetworkService;
	public AAC() throws ClassNotFoundException, SQLException{
		authorDSBService = new AuthorDSBService();
		citationNetworkService = new CitationNetworkService();
	}
	
	public void genPlainMatrix(String space, String save, String matrixType) throws ClassNotFoundException, SQLException, IOException{
		System.out.println("Generating plain matrix ...");
		if(adjacentMatrix ==null)
			adjacentMatrix = new AdjacentMatrix();
		Set<Integer> set = getSpace(space);
		adjacentMatrix.createFilteredMatrix(save, matrixType, set);
		System.out.println("Finished generating plain matrix.");
	}
	
	public static Set<Integer> getSpace(String space) throws IOException{
		Set<Integer> set = new HashSet<Integer>();
		FileInputStream fstream = new FileInputStream(space);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		while ((strLine = br.readLine()) != null){
			  set.add(Integer.parseInt(strLine));
		}
		System.out.println("size of space:"+set.size());
		return set;
	}
	
	public static List<String> getSpaceList(String space) throws IOException{
		List<String> list = new ArrayList<String>();
		FileInputStream fstream = new FileInputStream(space);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		while ((strLine = br.readLine()) != null){
			list.add(strLine);
		}
		in.close();
		fstream.close();
		return list;
	}
	
	public void getAuthorPairs(Set<Integer> articleIds, String save ) throws SQLException, IOException{
		FileWriter writer = new FileWriter(save);
		for(Integer articleId : articleIds){
			long pmid = citationNetworkService.getPMIDByArticleId(articleId);
			List<String> authors = authorDSBService.getAuthorsByPmid(pmid);
			for(String author: authors){
				writer.append(String.valueOf(articleId)).append(",\"").append(author).append("\"\n");
			}
		}
		writer.close();
	}
	
	

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
//		try {
//			SparseMatrix sm = new SparseMatrix();
//			System.out.println("Start generating matrices...");
//			sm.generateArticleMatrix();
//			sm.generateAuthorMatrix();
//			System.out.println("Task done. Matrices are generated.");
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
		//==================================
//		if(args.length != 3){
//			System.out.println("--space --save --matrix-type");
//			return;
//		}
//		AAC aac = new AAC();
//		try {
//			aac.genPlainMatrix(args[0], args[1], args[2]);
//			System.out.println("Task done.");
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		//===================================
		
		if(args.length != 2){
			System.out.println("--space --save");
			return;
		}
		AAC aac = new AAC();
		try {
			Set<Integer> set = aac.getSpace(args[0]);
			aac.getAuthorPairs(set, args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
