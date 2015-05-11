package classes;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, LongWritable, NodeWritable>{
	
	public void map(LongWritable key, Text value,
		      OutputCollector<LongWritable, NodeWritable> output, Reporter reporter)
		      throws IOException {
		    
		    String line = value.toString();
		    String[] splitData = line.split("\\t");
		    Integer nodeID = Integer.parseInt(splitData[0]);
		    String[] data = splitData[1].split("\\|");
		    //nodeID, pageRank, blockID, numOutEdges, ingoingEdgeId's, boundaryVertices
		    Float pageRank = Float.parseFloat(data[0]);
		    Integer blockID = Integer.parseInt(data[1]);
		    Integer numOutEdges = Integer.parseInt(data[2]);
		    String inGoingEdgesString = data[3];
		    String[] inGoingEdgeIDs = inGoingEdgesString.split("\\s+");
		    ArrayList<Long> inGoingEdges = new ArrayList<Long>();
		    for(String id: inGoingEdgeIDs){
		    	inGoingEdges.add(Long.parseLong(id));
		    }
		    String[] boundaryVertexString = data[4].split("\\s+");
		    ArrayList<Long> boundaryVertices = new ArrayList<Long>();
		    for(String bV: boundaryVertexString){
		    	Long boundaryVertexID = Long.parseLong(bV);
		    	boundaryVertices.add(boundaryVertexID);
		    }
		    
		    NodeWritable n = new NodeWritable(pageRank, nodeID, blockID, numOutEdges, inGoingEdges, boundaryVertices);
		    output.collect(new LongWritable(blockID), n);
		    for(Long boundaryVertexID: n.getBoundaryVertices()){
		    	Long boundaryVertexBlock = PageRank.blockIDofNode(boundaryVertexID);
		    	output.collect(new LongWritable(boundaryVertexBlock), n);
		    }
		    
		  }

}
