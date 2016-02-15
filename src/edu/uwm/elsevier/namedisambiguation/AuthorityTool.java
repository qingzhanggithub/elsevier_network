/**
 * 
 */
package edu.uwm.elsevier.namedisambiguation;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.uwm.elsevier.ITableNames;

import articlesdata.article.Author;
import articlesdata.article.AuthorService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class AuthorityTool {
	
	private ArticlesDataDBConnection databaseConnection;
	private AuthorityDatabaseConnection authorityDatabaseConnection;
	private AuthorService authorService;
	private int maxArticleId =25438518;
	private static String INSERT_MAPPING = 
			"insert into authority_map_v2 (authority_author_id, author_id, pmid, is_original) values(?, ?, ?, ?)";
	private PreparedStatement prepInsertMap ;
	private Logger logger = Logger.getLogger(AuthorityTool.class);

	public AuthorityTool() throws ClassNotFoundException, SQLException{
		databaseConnection = ArticlesDataDBConnection.getInstance();
		authorityDatabaseConnection = AuthorityDatabaseConnection.getInstance();
		authorService = new AuthorService();
		prepInsertMap = databaseConnection.getConnection().prepareStatement(INSERT_MAPPING);
	}
	
	public void mapAuthorNameToId(int start) throws SQLException, IOException{
		logger.info("Task started...");
		String sql =  "select article_id, pmid from "+ITableNames.ELSEVIER_PMID_MAPPING_TABLE;
		int lower = start;
		int page = 1000;
		int upper = lower + page;
		Statement stmt = databaseConnection.getConnection().createStatement();
		ResultSet rs = null;
//		FileWriter writer = new FileWriter(save);
		while(lower < maxArticleId){
			logger.info("lower="+lower);
			rs = stmt.executeQuery(sql+" where article_id >"+lower+" and article_id<="+upper);
			while(rs.next()){
				int articleId = rs.getInt(1);
				long pmid = rs.getLong(2);
				List<Author> authors = authorService.getAuthorListByArticleId(articleId);
				List<AuthorityEntity> entities = getAuthorIdsFromAuthority(pmid);
				for(AuthorityEntity entity: entities){ // entities will be modified if mapped
					for(Author author: authors){
						if(compareAuthorAndAuthorityEntity(author, entity)){
//							logger.info(entity.toString());
//							writer.append(entity.toString()).append('\n');
							insertMap(entity);
							continue;
						}
					}
				}
			}
			prepInsertMap.executeBatch();
			rs.close();
			lower = upper;
			upper += page;
		}
		prepInsertMap.close();
	}
	
	public void insertMap(AuthorityEntity entity) throws SQLException{
		prepInsertMap.setString(1, entity.getAuthorityId());
		prepInsertMap.setLong(2, entity.getAuthorId());
		prepInsertMap.setLong(3, entity.getPmid());
		prepInsertMap.setInt(4, entity.getIsOriginal());
		prepInsertMap.addBatch();
	}
	
	public static boolean isNamesEqual(Author src, Author dest){
		boolean isLastNameEqual = false;
		boolean isFirstNameEqual = true;
		if(src.getLastName() == null || dest.getLastName()== null){
			isLastNameEqual = false;
		}else if(src.getLastName().equalsIgnoreCase(dest.getLastName())){
			isLastNameEqual = true;
		}
		if(src.getFirstName() != null && dest.getFirstName()!=null){
			List<String> tokens = tokenizeFirstName(src.getFirstName());
			for(String token: tokens){
				if(dest.getFirstName().indexOf(token) == -1){
					isFirstNameEqual = false;
					break;
				}
			}
		}
		return isLastNameEqual&&isFirstNameEqual;
	}
	
	public boolean compareAuthorAndAuthorityEntity(Author author, AuthorityEntity entity){
		boolean isLastNameEqual = false;
		boolean isFirstNameEqual = false; 
		if(author.getLastName() != null){
			List<String> lastNames = entity.getLastNameVariations();
			for(String lastName: lastNames){
				if(lastName.equalsIgnoreCase(author.getLastName())){ 
					isLastNameEqual = true;
					break;
				}
			}
		}
		
		if(author.getFirstName() !=null){
			List<String> tokens = tokenizeFirstName(author.getFirstName());
			List<String> firstNames = entity.getFirstNameVariations();
			for(String firstName: firstNames){
				for(String token: tokens){
					if(firstName.indexOf(token)!= -1){
						isFirstNameEqual = true;
						break;
					}
				}
			}
		}
		
		if(isFirstNameEqual && isLastNameEqual){
			entity.setAuthorId(author.getAuthorId());
			entity.setArticleId(author.getArticleId());
			entity.setEmailAddress(author.getEmailAddress());
			entity.setCorrespondingAuthor(author.isCorrespondingAuthor());
		}
		
		return (isFirstNameEqual && isLastNameEqual);
	}
	
	public static List<String> tokenizeFirstName(String firstName){
		String[] names = firstName.split("[\\s.\\-]+");
		List<String> nameList = new ArrayList<String>();
		for(String name: names){
			if(name.trim().length()>0)
				nameList.add(name);
		}
		return nameList;
	}
	
	
	public List<AuthorityEntity> getAuthorIdsFromAuthority(long pmid) throws SQLException{
		String sql = "select authorID from Author_PMID where pmid=";
		Statement stmt = authorityDatabaseConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql+"\'"+pmid+"\'");
		List<AuthorityEntity> entityList = new ArrayList<AuthorityEntity>();
		while(rs.next()){
			AuthorityEntity entity = getAuthorByAuthorityId(rs.getString(1));
			entity.setPmid(pmid);
			entityList.add(entity);
		}
		return entityList;
	}
	
	public AuthorityEntity getAuthorByAuthorityId(String authorityId) throws SQLException{
		String escapedAuthorityId = escape(authorityId);
		String selectLastName = "select nameVariation from LastName where authorID=\'"+escapedAuthorityId+"\'";
		Statement stmt = authorityDatabaseConnection.getConnection().createStatement();
		List<String> lastNames = new ArrayList<String>();
//		logger.info("selectLastName:"+selectLastName);
		ResultSet rs = stmt.executeQuery(selectLastName);
		while(rs.next()){
			lastNames.add(rs.getString(1));
		}
		rs.close();
		String selectFirstName = "select nameVariation from FirstName where authorID=\'"+escapedAuthorityId+"\'";
		List<String> firstNames = new ArrayList<String>();
		rs = stmt.executeQuery(selectFirstName);
		while(rs.next()){
			firstNames.add(rs.getString(1));
		}
		AuthorityEntity entity = new AuthorityEntity();
		entity.setFirstNameVariations(firstNames);
		entity.setLastNameVariations(lastNames);
		entity.setAuthorityId(authorityId);
		return entity;
	}
	
	public static String escape(String org){
		char[] chs = org.toCharArray();
		StringBuffer sb = new StringBuffer();
		for(int i=0; i<chs.length; i++){
			if(chs[i]=='\'' || chs[i]== '\"'){
				sb.append('\\').append(chs[i]);
			}else
				sb.append(chs[i]);
		}
		return sb.toString();
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		
		AuthorityTool tool = new AuthorityTool();
		tool.mapAuthorNameToId(Integer.parseInt(args[0]));
//		String first = "Jong-Wook J.";
//		List<String> names = AuthorityTool.tokenizeFirstName(first);
//		for(String name: names){
//			System.out.println(name);
//		}
	}

}
