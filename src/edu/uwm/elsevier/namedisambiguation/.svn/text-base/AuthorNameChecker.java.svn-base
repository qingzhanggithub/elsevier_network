/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.csvreader.CsvReader;

import edu.uwm.elsevier.namedisambiguation.VectorBuilder.SimilarityInfo;

import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class AuthorNameChecker {

	private ArticlesDataDBConnection databaseConnection;
	private static String SELECT_AUTHOR_NAMES = "select author_id, last_name, first_name, author.article_id, pmid from author, elsevier_pmid_mapping " +
			"where author.article_id = elsevier_pmid_mapping.article_id and is_matched = 1 order by last_name, first_name";
	
	public AuthorNameChecker() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
	}
	
	public void printNames(int size, String save) throws SQLException, IOException{
		Statement stmt = databaseConnection.getConnection().createStatement(ResultSet.CONCUR_READ_ONLY, ResultSet.FETCH_FORWARD);
		stmt.setFetchSize(Integer.MIN_VALUE);
		String sql ;
		if(size == -1)
			sql = SELECT_AUTHOR_NAMES;
		else
			sql = SELECT_AUTHOR_NAMES + " limit "+size;
		 ResultSet rs = stmt.executeQuery(sql);
		 FileWriter writer = new FileWriter(save);
		 StringBuffer sb ;
		 int index = 0;
		 while(rs.next()){
			
			 sb = new StringBuffer();
			 sb.append(rs.getInt("author_id")).append('\t');
			 sb.append(rs.getString("last_name")).append('\t');
			 sb.append(rs.getString("first_name")).append('\t');
			 sb.append(rs.getInt("author.article_id")).append('\t');
			 sb.append(rs.getLong("pmid")).append('\t');
//			 System.out.println(index+"\t"+sb.toString());
			 writer.append(sb.toString()).append('\n');
			 index++;
			 if(index % 1000 == 0)
				 System.out.println(index+" have been processed.");
		 }
		 writer.close();
		 System.out.println(size+" author names output to "+size);
	}
	
	public void getAuthorFreq(String path) throws NumberFormatException, IOException{
		CsvReader csv = new CsvReader(path, '\t');
		List<AuthorVecInfo> listToCompare =null;
		AuthorVecInfo prev = null;
		AuthorVecInfo current = null;
		int index  =0;
		while(csv.readRecord()){
			System.out.println("in record "+index);
			if(prev== null && current==null// first row
					|| prev!= null && current!=null && !current.isEqual(prev)){	// new name
				if(listToCompare!=null){
					countWithinSameLastName(listToCompare);
					listToCompare.clear();
				}else
					listToCompare = new ArrayList<AuthorVecInfo>();
			}
			prev = current;
			int authorId = Integer.parseInt(csv.get(0));
			AuthorVecInfo authorVec = new AuthorVecInfo(authorId);
			authorVec.setLastName(csv.get(1));
			if(csv.get(2)!=null){	// separate first name from middle name
				HashMap<String, String> map = VectorBuilder.parseGivenName(csv.get(2));
				authorVec.setFirstName(map.get(VectorBuilder.FIRST_NAME_KEY));
				authorVec.setMiddleName(map.get(VectorBuilder.MIDDLE_NAME_KEY));
			}
			authorVec.setArticleId(Integer.parseInt(csv.get(3)));
			listToCompare.add(authorVec);
			current =authorVec;
			index ++;
		}
		System.out.println("Finished.");
	}
	
	public void countWithinSameLastName(List<AuthorVecInfo> authors){
//		writer.append("processing "+authors.get(0).getLastName());
		HashMap<String, List<AuthorVecInfo>> fullMap = new HashMap<String, List<AuthorVecInfo>>();
		HashMap<String, List<AuthorVecInfo>> initialMap = new HashMap<String, List<AuthorVecInfo>>();
		List<AuthorVecInfo> authorWithoutFirstNameList = new ArrayList<AuthorVecInfo>();
		List<AuthorVecInfo> authorWithFirstNameinitialList = new ArrayList<AuthorVecInfo>();
		List<AuthorVecInfo> uniqueList = new ArrayList<AuthorVecInfo>();// want to return
		for(AuthorVecInfo a: authors){
			if(a.getFirstName() == null){
				authorWithoutFirstNameList.add(a); // it will compare with every one else.
			}else if(a.getNameType(a.getFirstName()).equals("1")){
				VectorBuilder.addToMap(a.getFirstName(), a, fullMap);
				VectorBuilder.addToMap(String.valueOf(a.getFirstName().charAt(0)), a, initialMap);
			}else if(a.getNameType(a.getFirstName()).equals("0")){
				authorWithFirstNameinitialList.add(a);
			}
		}
		
		Set<String> firstNames = fullMap.keySet();
		for(AuthorVecInfo a: authorWithFirstNameinitialList){
			String firstName = a.getFirstName();
			List<AuthorVecInfo> initList = initialMap.get(firstName);
			String middle = a.getMiddleName();
			if(initList !=null){
				for(AuthorVecInfo target: initList){
					String targetMiddle = target.getMiddleName();
					if(!VectorBuilder.isMiddleNameEqual(middle, targetMiddle) )
						continue;
				}
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("--size --save");
			return;
		}
		AuthorNameChecker checker;
		try {
			checker = new AuthorNameChecker();
			checker.printNames(Integer.parseInt(args[0]), args[1]);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
