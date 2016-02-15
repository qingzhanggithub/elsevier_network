/**
 * 
 */
package edu.uwm.elsevier;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import articlesdata.article.Citation;
import articlesdata.database.inputformat.DBWritable;

/**
 * @author qing
 *
 */
public class RawCitation extends Citation implements DBWritable{
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		CitationId = resultSet.getLong("citation_qing.citation_id");
		citationText = resultSet.getString("citation_text");
	}
}
