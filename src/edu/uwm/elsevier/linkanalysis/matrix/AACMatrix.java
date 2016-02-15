/**
 * 
 */
package edu.uwm.elsevier.linkanalysis.matrix;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author qing
 *
 */
public class AACMatrix {
	
	
	public static String AAC_ROOT = "/home/qzhang/subgraphs/";
	
	public AACMatrix(){
		
	}
	
	public static Map<String, Integer> loadIndexMap(String idPath) throws IOException{
		Map<String, Integer> map = new HashMap<String, Integer>();
		List<String> spaceList = AAC.getSpaceList(idPath);
		for(int i=0; i<spaceList.size(); i++){
			map.put(spaceList.get(i), i+1); // start from 1!!
		}
		return map;
	}
	
	public static void constructArticleMatrix(String pairPath, String idPath) throws IOException{
		System.out.println("Constructing article matrix ...");
		Map<String, Integer> map = loadIndexMap(AAC_ROOT+idPath);
		int dim =map.size();
//		int[][] matrix = new int[dim][dim];
		FileWriter writer = new FileWriter(AAC_ROOT+"article_matrix.csv");
		FileInputStream fstream = new FileInputStream(AAC_ROOT+pairPath);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		while ((strLine = br.readLine()) != null){
			String[] record = strLine.split("\\s");
			if(record.length >=2 && record[0].length()>0 && record[1].length()>0){
				int srcIndex = map.get(record[0]);
				String[] dests = record[1].split(",");
				if(dests.length >0){
					for(int i=0; i<dests.length; i++){
						if(dests[i].length() >0){
							Integer destIndex = map.get(dests[i]);
//							matrix[srcIndex][destIndex] = 1;
							writer.append(String.valueOf(srcIndex)).append(',').append(String.valueOf(destIndex)).append(",1\n");
						}
					}
				}
			}
		}
		writer.append(String.valueOf(dim)).append(',').append(String.valueOf(dim)).append(',').append("0\n");
		br.close();
		in.close();
		fstream.close();
		writer.close();
	}
	
	public static void constructAuthorMatrix(String pairPath, String articleIdPath, String authorIdPath) throws IOException{
		System.out.println("Constructing author matrix ...");
		Map<String, Integer> articleMap = loadIndexMap(AAC_ROOT+articleIdPath);
		int row = articleMap.size();
		Map<String, Integer> authorMap = loadIndexMap(AAC_ROOT+authorIdPath);
		int col = authorMap.size();
//		int[][] matrix = new int[articleMap.size()][authorMap.size()];
		FileWriter writer = new FileWriter(AAC_ROOT+"author_matrix.csv");
		FileInputStream fstream = new FileInputStream(AAC_ROOT+pairPath);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		while ((strLine = br.readLine()) != null){
			String[] record = strLine.split(",");
			if(record.length >=2 && record[0].length()>0 && record[1].length()>0){
				int articleIndex = articleMap.get(record[0]);
				int authorIndex = authorMap.get(record[1]);
//				matrix[articleIndex][authorIndex] = 1;
				writer.append(String.valueOf(articleIndex)).append(',').append(String.valueOf(authorIndex)).append(",1\n");
			}
		}
		writer.append(String.valueOf(row)).append(',').append(String.valueOf(col)).append(",0\n");
		br.close();
		in.close();
		fstream.close();
		writer.close();
	}
	
	
	public static void outputMatrix(int[][] matrix, String save) throws IOException{
		System.out.println("Writing matrix to "+save);
		FileWriter writer = new FileWriter(save);
		for(int i=0; i<matrix.length; i++){
			boolean isFirst = true;
			for(int j=0; j<matrix[i].length; j++){
				if(isFirst){
					writer.append(String.valueOf(matrix[i][j]));
					isFirst = false;
				}else{
					writer.append(",").append(String.valueOf(matrix[i][j]));
				}
			}
			writer.append("\n");
		}
		writer.close();
		System.out.println("Finished writing matrix.");
	}
	

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if(args.length==0 || args[0].equals("--help")){
			System.out.println("--root --matrix-type(article/author) --pair-path --id-path [..--id-path]");
			return;
		}
		AACMatrix.AAC_ROOT = args[0];
		if(args[1].equals("article")){
			AACMatrix.constructArticleMatrix(args[2], args[3]);
		}else{
			AACMatrix.constructAuthorMatrix(args[2], args[3], args[4]);
		}

	}

}
