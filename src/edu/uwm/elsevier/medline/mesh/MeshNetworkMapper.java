/**
 * 
 */
package edu.uwm.elsevier.medline.mesh;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.uwm.elsevier.medline.CitationNetworkEdge;

/**
 * @author qing
 *
 */
public class MeshNetworkMapper extends Mapper<LongWritable, CitationNetworkEdge, MeshNetworkEdge, IntWritable> {

	private MeshNetworkBuilder builder;
	private static Logger LOGGER = Logger.getLogger("MeshNetworkMapper");
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		try {
			builder = new MeshNetworkBuilder();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
	}
	

	@Override
	protected void map(LongWritable key, CitationNetworkEdge value, Context context)throws IOException, InterruptedException {
		try {
			List<MeshNetworkEdge> edges = builder.buildMeshCitationLink(value);
			for(MeshNetworkEdge edge: edges){
				context.write(edge, new IntWritable(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
		
	}

	
	
	
	

}
