/**
 * 
 */
package edu.uwm.elsevier;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import articlesdata.article.Article;
import articlesdata.article.ArticleService;
import articlesdata.article.AuthorService;
import articlesdata.article.Citation;
import articlesdata.citation.Author;
import articlesdata.citation.CitationParserUtils;
import articlesdata.citation.CitationService;

import edu.uwm.elsevier.indexer.ElsevierIndexer;

/**
 * @author qing
 *
 */
public class ElsevierArticleMetaDataSearcher {
	
	protected int NUM_OF_SEARCH_RESTURN = 20;
//	public static String ELSEVIER_INDEX_PATH_DISTRIBUTED  = "/user/qzhang/elsevier_index/";
	public static char[] ESCAPE_FOR_QUERY = new char[]{'\\', '+', '-', '!', '(', ')', ':', '^','[', ']', '{', '}', '~', '*', '?', '\"'};
	public static String TEXT_TOKENIZE_PATTERN = "[\\s,.:;\\(\\)\"\\*%$\\+/\\[\\]=#&\\?]";
	public static int MAX_TOKEN_IN_QUERY = 512;
	private static HashSet<Character> ESCAPE_SET = null;
//	protected QueryParser parser = new QueryParser(Version.LUCENE_35, "title", new StandardAnalyzer(Version.LUCENE_35));
	protected IndexSearcher searcher;
	protected HashMap<Integer, List<articlesdata.article.Author>> authorMap;
	private CitationNetworkService citationNetworkService;
	
	
	
	public ElsevierArticleMetaDataSearcher(String indexPath) throws IOException, ClassNotFoundException, SQLException{
		NIOFSDirectory directory;
		directory = new NIOFSDirectory(new File(indexPath));
		IndexReader reader = IndexReader.open(directory, true);
		searcher = new IndexSearcher(reader);
		citationNetworkService = new CitationNetworkService();
	}
	
	public static HashSet<Character> getEscapeSet(){
		if(ESCAPE_SET == null){
			ESCAPE_SET = new HashSet<Character>();
			for(char c: ESCAPE_FOR_QUERY){
				ESCAPE_SET.add(c);
			}
		}
		return ESCAPE_SET;
	}
	
	public static String cleanStringForQuery(String str){
		int size = str.length();
		char[] chars = str.toCharArray();
		for(int i=0; i<size; i++){
			if(getEscapeSet().contains(chars[i])){
				chars[i] = ' ';
			}
		}
		return String.valueOf(chars);
	}
	
	public static BooleanQuery getQuerySimple(String text){
		BooleanQuery query = new BooleanQuery();
		if(text == null)
			return query;
		BooleanQuery titleQuery = new BooleanQuery();
		BooleanQuery authorQuery = new BooleanQuery();
		ArrayList<String> tokens = CitationParserUtils.simpleTokenization(text, TEXT_TOKENIZE_PATTERN);
		if(tokens.size()>MAX_TOKEN_IN_QUERY)	//too long. not a valid citation.
			return query;
		for(String token: tokens){
			titleQuery.add(new TermQuery(new Term(ElsevierIndexer.TITLE_FIELD, token)), Occur.SHOULD);
			authorQuery.add(new TermQuery(new Term(ElsevierIndexer.AUTHORS_FIELD, token)), Occur.SHOULD);
		}
		query.add(titleQuery, Occur.SHOULD);
		query.add(authorQuery, Occur.SHOULD);
		return query;
	}
	
	public static BooleanQuery getQuery(String title, List<Author> authors){

		BooleanQuery query = null;
		BooleanQuery titleQuery = getQueryByTitle(title);
		BooleanQuery authorQuery = getQueryByCitationAuthors(authors);
		
		if(titleQuery !=null && authorQuery !=null){
			query = new BooleanQuery();
			query.add(titleQuery, Occur.SHOULD);
			query.add(authorQuery, Occur.SHOULD);
		}else if(titleQuery !=null){
			query = titleQuery;
		}else if(authorQuery !=null){
			query =authorQuery;
		}
		return query;
	}
	
	public static BooleanQuery getQueryFromAritcle(String title, List<articlesdata.article.Author> authors){
		BooleanQuery query = null;
		BooleanQuery titleQuery = getQueryByTitle(title);
		BooleanQuery authorQuery = getQueryByArticleAuthors(authors);
		if(titleQuery !=null && authorQuery !=null){
			query = new BooleanQuery();
			query.add(titleQuery, Occur.SHOULD);
			query.add(authorQuery, Occur.SHOULD);
		}else if(titleQuery !=null){
			query = titleQuery;
		}else if(authorQuery !=null){
			query =authorQuery;
		}
		return query;
	}
	
	public static BooleanQuery getQueryByTitle(String title){
		BooleanQuery titleQuery = null;
		if(title !=null){
			titleQuery = new BooleanQuery();
			ArrayList<String> tokens = CitationParserUtils.simpleTokenization(title, TEXT_TOKENIZE_PATTERN);
			for(String token: tokens){
				titleQuery.add(new TermQuery(new Term(ElsevierIndexer.TITLE_FIELD, token)), Occur.SHOULD);
			}
		}
		return titleQuery;
	}
	
	public static BooleanQuery getQueryByCitationAuthors(List<articlesdata.citation.Author> authors){
		BooleanQuery authorQuery = null;
		if(authors !=null){
			authorQuery = new BooleanQuery();
			for(Author author: authors){
				if(author.getSurname() !=null)
					authorQuery.add(new TermQuery(new Term(ElsevierIndexer.AUTHORS_FIELD, author.getSurname().toLowerCase())), Occur.SHOULD);// need toLowerCase!!
			}
		}
		return authorQuery;
	}
	
	public static BooleanQuery getQueryByArticleAuthors(List<articlesdata.article.Author> authors){
		BooleanQuery authorQuery = null;
		if(authors !=null){
			authorQuery = new BooleanQuery();
			for(articlesdata.article.Author author: authors){
				if(author.getLastName() !=null)
					authorQuery.add(new TermQuery(new Term(ElsevierIndexer.AUTHORS_FIELD, author.getLastName().toLowerCase())), Occur.SHOULD);// need toLowerCase!!
			}
		}
		return authorQuery;
	}
	
	public List<Article> getArticleIds(String title, List<Author> authors) throws ParseException, IOException, SQLException{
		BooleanQuery query =getQuery(title, authors);
		return getArticleIdsByQuery(query);
	}
	
	public List<Article> getArticleIdsSimple(String text) throws IOException, SQLException{
		BooleanQuery query = getQuerySimple(text);
		return getArticleIdsByQuery(query);
	}
	
	public List<Article> getArticleIdsByQuery(Query query) throws IOException, SQLException{
		List<Article> articles  = new ArrayList<Article>();
		
		if(query ==null)
			return articles; //no query available. return empty article. 
		authorMap = new HashMap<Integer, List<articlesdata.article.Author>>();
		TopDocs hits  = searcher.search(query, NUM_OF_SEARCH_RESTURN);
		ScoreDoc[] docs = hits.scoreDocs;
		
		if(docs!=null ){
			for(int i=0; i<docs.length; i++){
				Document document = searcher.getIndexReader().document(docs[i].doc);
				String articleIdStr = document.get(ElsevierIndexer.ARTICLE_ID_FIELD);
				String articleTitle = document.get(ElsevierIndexer.TITLE_FIELD);
				if(articleTitle.length()==0)
					articleTitle = null;
				else
					articleTitle = StringEscapeUtils.unescapeHtml(articleTitle);
				String identifier = document.get(ElsevierIndexer.IDENTIFIER_FIELD);
				if(identifier.length() == 0)
					identifier = null;
				String sourceIdStr= document.get(ElsevierIndexer.SOURCE_ID_FIELD);
				String journal = document.get(ElsevierIndexer.JOURNAL_FIELD);
				if(journal.length() ==0)
					journal = null;
				else
					journal = StringEscapeUtils.unescapeHtml(journal);
				String authorStrs = document.get(ElsevierIndexer.AUTHORS_FIELD);
				Article article = new Article();
				article.setArticleId(Integer.parseInt(articleIdStr));
				article.setTitle(articleTitle);
				article.setJournal(journal);
				article.setSourceId(Integer.parseInt(sourceIdStr));
				article.setIdentifier(identifier);
				article.setDatePublished(citationNetworkService.getArticleYearByArticleId(article.getArticleId()));
				List<articlesdata.article.Author> authorList = parseAuthorField(authorStrs);
				authorMap.put(article.getArticleId(), authorList);
				
				articles.add(article);
			}
		}
		return articles;
	}
	
	public List<ElsevierArticle> getElsevierArticlesByQuery(Query query) throws IOException{
		List<ElsevierArticle> articles  = new ArrayList<ElsevierArticle>();
		
		if(query ==null)
			return articles; //no query available. return empty article. 
		TopDocs hits  = searcher.search(query, NUM_OF_SEARCH_RESTURN);
		ScoreDoc[] docs = hits.scoreDocs;
		
		if(docs!=null ){
			for(int i=0; i<docs.length; i++){
				Document document = searcher.getIndexReader().document(docs[i].doc);
				String articleIdStr = document.get(ElsevierIndexer.ARTICLE_ID_FIELD);
				String articleTitle = document.get(ElsevierIndexer.TITLE_FIELD);
				if(articleTitle.length()==0)
					articleTitle = null;
				else
					articleTitle = StringEscapeUtils.unescapeHtml(articleTitle);
				String identifier = document.get(ElsevierIndexer.IDENTIFIER_FIELD);
				if(identifier.length() == 0)
					identifier = null;
				String sourceIdStr= document.get(ElsevierIndexer.SOURCE_ID_FIELD);
				String journal = document.get(ElsevierIndexer.JOURNAL_FIELD);
				if(journal.length() ==0)
					journal = null;
				else
					journal = StringEscapeUtils.unescapeHtml(journal);
				String authorStrs = document.get(ElsevierIndexer.AUTHORS_FIELD);
				ElsevierArticle article = new ElsevierArticle();
				article.setArticleId(Integer.parseInt(articleIdStr));
				article.setTitle(articleTitle);
				article.setJournal(journal);
				article.setSourceId(Integer.parseInt(sourceIdStr));
				article.setIdentifier(identifier);
				
				List<articlesdata.article.Author> authorList = parseAuthorField(authorStrs);
				article.setAuthors(authorList);
				articles.add(article);
			}
		}
		return articles;
	}
	
	public void close() throws IOException{
		searcher.close();
	}
	
	public static List<articlesdata.article.Author> parseAuthorField(String authorsStr){
		List<articlesdata.article.Author> authorList = new ArrayList<articlesdata.article.Author>();
		if(authorsStr ==null)
			return authorList;// return empty list;
		authorsStr = StringEscapeUtils.unescapeHtml(authorsStr);
		String[] names = authorsStr.split(ElsevierIndexer.NAME_DELIMITER);
		for(String name: names){
			String trimed = name.trim();
			if(trimed.length() ==0)
				continue;
			articlesdata.article.Author author = new articlesdata.article.Author();
			String[] parts = trimed.split("\\"+ElsevierIndexer.FIRST_LAST_NAME_DELIMITER);
			boolean isSurnameFound = false;
			for(int i=parts.length-1; i>=0; i--){
				if(parts[i].trim().length() !=0 ){
					if(!isSurnameFound){
						author.setLastName(parts[i].trim());
						isSurnameFound = true;
					}else{
						author.setFirstName(parts[i].trim());
					}
				}
			}
			authorList.add(author);
		}
		return authorList;
	}
	
	public static List<articlesdata.citation.Author> parseAuthorFieldIntoCitationAuthors(String authorsStr){
		List<articlesdata.citation.Author> authorList = new ArrayList<Author>();
		if(authorsStr == null)
			return authorList;
		String[] names = authorsStr.split(ElsevierIndexer.NAME_DELIMITER);
		for(String name: names){
			String trimed = name.trim();
			if(trimed.length() ==0)
				continue;
			articlesdata.citation.Author author = new Author();
			String[] parts = trimed.split("\\"+ElsevierIndexer.FIRST_LAST_NAME_DELIMITER);
			boolean isSurnameFound = false;
			for(int i=parts.length-1; i>=0; i--){
				if(parts[i].trim().length() !=0 ){
					if(!isSurnameFound){
						author.setSurname(parts[i].trim());
						isSurnameFound = true;
					}else{
						author.setGivenNameString(parts[i].trim());
					}
				}
			}
			authorList.add(author);
		}
		return authorList;
	}
	
	
	
	public HashMap<Integer, List<articlesdata.article.Author>> getAuthorMap() {
		return authorMap;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String org = "Phase I study in melanoma patients of a vaccine with peptide-pulsed dendritic cells generated in vitro from CD34(+) hematopoietic progenitor cells";
////		String clean = ElsevierArticleMetaDataSearcher.cleanStringForQuery(org);
//		String authorStr = "Mohammed$Moumnassi;Salim$Belouettar;&Eacute;ric$B&eacute;chet;St&eacute;phane P.A.$Bordas;Didier$Quoirin;Michel$Potier-Ferry;";
//		authorStr ="A.$Anisimov;W.$Buchm&uuml;ller;M.$Drewes;S.$Mendizabal;";
//		authorStr ="Shelly D.$Kelly;Peng$Lu;Trudy$Bolin;Soma$Chattopadhyay;Matthew G.$Newville;Tomohiro$Shibata;Chen$Zhu;";
//		authorStr ="Rita$Cunha;Carlos$Silvestre;Jo&atilde;o$Hespanha;A.$Pedro Aguiar;";
//		List<articlesdata.article.Author> authors = parseAuthorField(authorStr);
//		for(articlesdata.article.Author a: authors){
//			System.out.println(a.getFirstName()+"\t"+a.getLastName());
//		}
		
		String c= "Science";
		String a = "Social Science Research";
		int articleId = 0;
		long citationId = 16961459;
		citationId = 48097610;
		
		try {
			ArticleService articleService = new ArticleService();
			CitationService citationService = new CitationService();
			AuthorService authorService = new AuthorService();
			Citation citation = citationService.getCitationByCitationId(citationId);
			String fakeTitle = "";
			List<Author> fakeAuthors =new  ArrayList<Author>();
			Author fakeA = new Author();
			fakeAuthors.add(fakeA);
			ElsevierArticleMetaDataSearcher searcher = new ElsevierArticleMetaDataSearcher("/Users/qing/wind3/elsevier_index");
			List<Article> articleIds = searcher.getArticleIds(fakeTitle, fakeAuthors);
			for(Article ar: articleIds){
				System.out.println(ar.getArticleId()+"\t"+ar.getTitle()+"\t"+ar.getJournal());
			}
			
			//			MappingStatus status = new MappingStatus(citationId, articleId);
//			CitationArticleComparison.isCitationAndArticleEqual(citation, article, authorsA, status);
//			System.out.println("title\t"+status.getTitleComp()
//					+"author\t"+status.getAuthorsComp()
//					+"journal\t"+status.getJournalComp());
			
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
//		System.out.println(clean);
		
	}

}
