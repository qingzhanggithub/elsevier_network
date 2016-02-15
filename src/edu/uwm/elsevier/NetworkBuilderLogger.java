/**
 * 
 */
package edu.uwm.elsevier;

import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * @author qing
 *
 */
public class NetworkBuilderLogger {
	private static String LOGGER_FILE_PATH = "/home/qzhang/netbuilder.log";
	private static String CITATION_INDEXER_LOGGER_FILE_PATH = "/home/qzhang/elsevier_citation_inexer.log";
	private static String DB_CHECKER = "/home/qzhang/dbcheck.log";
	private static String AUTHOR_DSB = "/home/qzhang/authordsb.log";
	private static String CITATION_MERGER = "/home/qzhang/citation_merger.log";
	private static Logger logger;
	
	public static Logger getLogger(String name){
		if(logger !=null)
			return logger;
		logger = Logger.getLogger(name);
		try {
		    // Create a file handler that write log record to a file called my.log
		    String pattern = "%d{dd MMM yyyy HH:MM:ss } %C{2} %p : %m %n";
		    PatternLayout layout = new PatternLayout(pattern);
		    FileAppender fileAppender;
		    if(name.equalsIgnoreCase("CitationIndexer")){
		    	fileAppender = new FileAppender(layout, CITATION_INDEXER_LOGGER_FILE_PATH);
		    	logger.setLevel(Level.ALL);
		    }else if(name.equalsIgnoreCase("DBChecker")){
		    	fileAppender = new FileAppender(layout, DB_CHECKER);
		    	logger.setLevel(Level.ALL);
		    }else if(name.equalsIgnoreCase("AuthorDSB")){
		    	fileAppender = new FileAppender(layout, AUTHOR_DSB);
		    	logger.setLevel(Level.ALL);
		    }else if(name.equalsIgnoreCase("CitationMerger")){
		    	fileAppender = new FileAppender(layout, CITATION_MERGER);
		    	logger.setLevel(Level.ALL);
		    }
		    else{
		    	fileAppender = new FileAppender(layout, LOGGER_FILE_PATH);
		    	logger.setLevel(Level.ERROR);
		    }
		    // Add to the desired logger
		    logger.addAppender(fileAppender);
		} catch (IOException e) {
		}
		return logger;
	}
}
