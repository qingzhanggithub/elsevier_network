/**
 * 
 */
package edu.uwm.elsevier.indexer;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import edu.uwm.elsevier.NetworkBuilderLogger;
import articlesdata.article.Citation;
import articlesdata.citation.Author;
import articlesdata.citation.CitationService;
import articlesdata.database.ArticlesDataDBConnection;

/**
 * @author qing
 *
 */
public class ElsevierCitationIndexer {

	
	public static String ELSEVIER_CITATION_INDEX_PATH = "/home/qzhang/elsevier_citation_index_2013_v2";
	public static String CITATION_ID_FIELD = "citation_id";
	public static String DATE_FIELD = "date";
	public static String IS_ORIGINALLY_PARSED_FIELD = "is_originally_parsed";
	public static String FIRST_PAGE_FIELD = "first_page";
	public static String LAST_PAGE_FIELD = "last_page";
	public static String ISSUE_FIELD = "issue";
	public static String CITATION_KEY_FIELD = "citation_key";
	public static String CITATION_TEXT_FIELD ="citation_text";
	
	private ArticlesDataDBConnection conn ;
	private Logger LOGGER = Logger.getLogger(ElsevierCitationIndexer.class);
	private static String INSERT_STATUS = "insert into elsevier_citation_index_status (citation_id, status, ddate) values (?,?,?)";
	private static String SELECT_STATUS = "select status from elsevier_citation_index_status where citation_id = ";
	private PreparedStatement prepInsertStatus;
	private CitationService citationService;
	private IndexWriter writer ;
	
	
	public ElsevierCitationIndexer() throws IOException{
		NIOFSDirectory directory = new NIOFSDirectory(new File(ELSEVIER_CITATION_INDEX_PATH));
		IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35));
		writer = new IndexWriter(directory, conf);
	}
	/**
	 * Index a citation instance. there may have encoding issue for languages other than English.  UTF-8 is enforced by jvm parameter, but the quality is still unclear on this issue.
	 * @param citation
	 * @throws CorruptIndexException
	 * @throws IOException
	 */
	public void indexCitation(Citation citation) throws CorruptIndexException, IOException{
		Document doc = new Document();
		doc.add(new Field(ElsevierIndexer.TITLE_FIELD, citation.getTitle()==null?"":StringEscapeUtils.unescapeHtml(citation.getTitle()), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
		doc.add(new Field(ElsevierIndexer.JOURNAL_FIELD, citation.getSource()==null?"":StringEscapeUtils.unescapeHtml(citation.getSource()),Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
		List<Author> authors = citation.getAuthors();
		StringBuilder sb = new StringBuilder();
		for(Author a : authors){	
			if(a.getGivenNameString()!=null)
				sb.append(a.getGivenNameString()).append(" $ ");
			if(a.getSurname()!=null)
				sb.append(a.getSurname()).append("; ");
		}
		String name = sb.toString();
		doc.add(new Field(ElsevierIndexer.AUTHORS_FIELD,name,Field.Store.YES,Field.Index.ANALYZED_NO_NORMS, Field.TermVector.WITH_POSITIONS_OFFSETS));
		
		doc.add(new Field(CITATION_TEXT_FIELD, citation.getCitationText()==null?"":StringEscapeUtils.unescapeHtml(citation.getCitationText()), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
		doc.add(new Field(CITATION_KEY_FIELD, citation.getCitationKey()==null?"":citation.getCitationKey(), Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(IS_ORIGINALLY_PARSED_FIELD, String.valueOf(citation.isOriginallyParsed()), Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(CITATION_ID_FIELD, String.valueOf(citation.getCitationId()), Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(ElsevierIndexer.ARTICLE_ID_FIELD, String.valueOf(citation.getArticleId()), Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(FIRST_PAGE_FIELD, String.valueOf(citation.getFirstPage()), Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(LAST_PAGE_FIELD, String.valueOf(citation.getLastPage()), Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field(ISSUE_FIELD, String.valueOf(citation.getIssue()), Field.Store.YES, Field.Index.NOT_ANALYZED));
		writer.addDocument(doc);
	}
	/**
	 * Read citation information from citation_detail table and index them by lucene. The primary purpose is  to speed up matching process.
	 * I comment out all the status checking by db, and the index is speed is much higher.
	 * @param start starting row. mainly used for resume the program.
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws CorruptIndexException
	 * @throws IOException
	 */
	public void indexCitationFromDB(int start) throws ClassNotFoundException, SQLException, CorruptIndexException, IOException{
		System.out.println("====Indexer Started====");
		System.out.println(ELSEVIER_CITATION_INDEX_PATH);
		int total = 0;
		conn = ArticlesDataDBConnection.getInstance();
		prepInsertStatus = conn.getConnection().prepareStatement(INSERT_STATUS);
		citationService = new CitationService();
		Statement stmt = conn.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery("select max(citation_id) from citation_qing");
		rs.next();
		long max = rs.getLong(1);
		rs.close();
		int low = start;
		int page = 1000;
		int upper = low+ page;
		while(low <= max){
			System.out.println(total+" have been indexed. low= "+low);
			String condition_main = " c.citation_id >= " +low +" AND c.citation_id < "+ upper;
			
			String sql = "select * from citation_qing as c, citation_detail as cd where "
					+condition_main+" AND c.citation_id = cd.citation_id ";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				Citation citation = citationService.mapResultSetToCitation(rs);
				indexCitation(citation);
				total++;
				citation = null;
			}
			writer.commit();	// is it correct to use in this way?
			
			rs.close();
			low = upper;
			upper += page;
		}
		writer.close();
		conn.close();
		System.out.println("===Task Done "+total+" indexed ====");
	}
	
	
	private void insertStatus(long citationId, int status) throws SQLException{
		prepInsertStatus.setLong(1, citationId);
		prepInsertStatus.setInt(2, status); //1, success; 0- failed.
		prepInsertStatus.setDate(3, new java.sql.Date(Calendar.getInstance().getTimeInMillis()));
		prepInsertStatus.executeUpdate();
	}
	
	private boolean hasIndexByCheckingStatus(long citationId) throws SQLException{
		Statement stmt = conn.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(SELECT_STATUS+citationId);
		boolean hasIndex = rs.next();
		rs.close();
		return hasIndex;
	}
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args.length !=1){
			System.out.println("Elsevier Citation Indexer\n--start");
			return;
		}

		try {
			ElsevierCitationIndexer indexer = new ElsevierCitationIndexer();
			try {
				indexer.indexCitationFromDB(Integer.parseInt(args[0]));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
