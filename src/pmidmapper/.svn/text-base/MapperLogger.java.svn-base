/**
 * 
 */
package pmidmapper;

import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


/**
 * @author qing
 *
 */
public class MapperLogger {
private static Logger logger;
	
	public static Logger getLogger(String name){
		if(logger !=null)
			return logger;
		logger = Logger.getLogger(name);
		try {
		    // Create a file handler that write log record to a file called my.log
		    logger.setLevel(Level.ALL);
		    String pattern = "%d{dd MMM yyyy HH:MM:ss } %C{2} %p : %m %n";
		    PatternLayout layout = new PatternLayout(pattern);
		    FileAppender fileAppender = new FileAppender(layout, "/home/qzhang/elsevier_pmid_mapper_logger.txt");
		    // Add to the desired logger
		    logger.addAppender(fileAppender);
		} catch (IOException e) {
		}
		return logger;
	}

}
