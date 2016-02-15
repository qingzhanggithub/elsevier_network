/**
 * 
 */
package pmidmapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import articlesdata.article.Article;
import articlesdata.article.Author;
import articlesdata.database.inputformat.DBWritable;

/**
 * @author qing
 *
 */
public class PMArticle extends Article implements DBWritable{

	private long pmid;
	private List<Author> authorList;
	private String year;
	private String meshs;
	private String abs;
	
	
	public long getPmid() {
		return pmid;
	}
	public void setPmid(long pmid) {
		this.pmid = pmid;
	}
	public List<Author> getAuthorList() {
		return authorList;
	}
	public void setAuthorList(List<Author> authorList) {
		this.authorList = authorList;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public String getMeshs() {
		return meshs;
	}
	public void setMeshs(String meshs) {
		this.meshs = meshs;
	}
	
	public String getAbs() {
		return abs;
	}
	public void setAbs(String abs) {
		this.abs = abs;
	}
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		articleId = rs.getInt("article_id");
		pmid = rs.getLong("pmid");
	}

}
