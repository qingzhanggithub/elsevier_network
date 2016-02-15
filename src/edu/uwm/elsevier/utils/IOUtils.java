/**
 * 
 */
package edu.uwm.elsevier.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @author qing
 *
 */
public class IOUtils {
	
	public static Set<String> readLineAsStringSet(String path) throws IOException{
		FileInputStream fstream = new FileInputStream(path);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		Set<String> set = new HashSet<String>();
		while ((strLine = br.readLine()) != null){
			set.add(strLine);
		}
		br.close();
		in.close();
		fstream.close();
		return set;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
