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
	implements Mapper<LongWritable, Text, LongWritable, Node>{
	
	public void map(LongWritable key, Text value,
		      OutputCollector<LongWritable, Node> output, Reporter reporter)
		      throws IOException {
		    
		    String line = value.toString();
		    String[] data = line.split("|");
		    //nodeID, pageRank, blockID, numOutEdges, ingoingEdgeId's, boundaryVertices
		    Integer nodeID = Integer.parseInt(data[0]);
		    Long pageRank = Long.parseLong(data[1]);
		    Integer blockID = Integer.parseInt(data[2]);
		    Integer numOutEdges = Integer.parseInt(data[3]);
		    String inGoingEdgesString = data[4];
		    String[] inGoingEdgeIDs = inGoingEdgesString.split("\\s+");
		    ArrayList<Long> inGoingEdges = new ArrayList<Long>();
		    for(String id: inGoingEdgeIDs){
		    	inGoingEdges.add(Long.parseLong(id));
		    }
		    String[] boundaryVertexString = data[5].split("\\s+");
		    ArrayList<Long> boundaryVertices = new ArrayList<Long>();
		    for(String bV: boundaryVertexString){
		    	Long boundaryVertexID = Long.parseLong(bV);
		    	boundaryVertices.add(boundaryVertexID);
		    }
		    
		    Node n = new Node(pageRank, nodeID, blockID, numOutEdges, inGoingEdges, boundaryVertices);
		    output.collect(new LongWritable(blockID), n);
		    for(Long boundaryVertexID: n.getBoundaryVertices()){
		    	Long boundaryVertexBlock = PageRank.blockIDofNode(nodeID);
		    	output.collect(new LongWritable(boundaryVertexBlock), n);
		    }
		    
		  }

}
