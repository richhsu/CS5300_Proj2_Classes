package classes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements
		Reducer<LongWritable, NodeWritable, LongWritable, Text> {

	@Override
	public void reduce(LongWritable arg0, Iterator<NodeWritable> arg1,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		
		// Stores all the nodes that have been given to this reducer
		Hashtable<Long, Node> allNodes = new Hashtable<Long, Node>();

		ArrayList<Long> blockNodes = new ArrayList<Long>();
		// Stores all the pageranks of the nodes
		HashMap<Long, Float> newNodePageRanks = new HashMap<Long, Float>();
		
		while (arg1.hasNext()) {
			NodeWritable currentNodeWritable = arg1.next();
			Node currentNode = currentNodeWritable.createNode();
			allNodes.put(currentNode.getNodeID(), currentNode);			
			if (currentNode.getblockID() == arg0.get()) {
				// belongs to the block
				blockNodes.add(currentNode.getNodeID());
				newNodePageRanks.put(currentNode.getNodeID(), (float)(0));
			}
			
		}
		

		HashMap<Long, Float> initialPageRanks = new HashMap<Long, Float>();
		for (Long nodeID : blockNodes) {
			initialPageRanks.put(nodeID, allNodes.get(nodeID).getPageRank());
		}

		Float residualError = (float) (1);
		int numIterations = 0;

		while (Math.abs(residualError) >= 0.001) {
			residualError = (float) (0);
			numIterations++;
			if(numIterations > 30) break;
			//calculate new page ranks
			for (Long nodeID : blockNodes) {
				Node n = allNodes.get(nodeID);
				Float newPageRank = (float)(0);
				for (Long inGoingEdgeID : n.getInGoingEdges()) {
					Node inGoingNode = allNodes.get(inGoingEdgeID);
					newPageRank += (inGoingNode.getPageRank() / inGoingNode.getNumOutEdges());
				}
				newNodePageRanks.put(nodeID, newPageRank + (float) ((1 - 0.85) / allNodes.size()));
				residualError += (allNodes.get(nodeID).getPageRank() - newPageRank)/newPageRank;
			}
			//update to new page ranks
			for(Long nodeID: blockNodes){
				allNodes.get(nodeID).setPageRank(newNodePageRanks.get(nodeID));
			}
		}
		
		Float netResidualError = (float) (0);
		for (Long nodeID: initialPageRanks.keySet()){
			
			Node newNode = allNodes.get(nodeID);
			
			Float initialPageRank = initialPageRanks.get(nodeID);
			
			long nID = newNode.getNodeID();
			float newPageRank = newNode.getPageRank();
			long bID = newNode.getblockID();
			int numOutEdge = newNode.getNumOutEdges();

			String nodeWithUpdatedPR = newPageRank + "|" + bID + "|" + numOutEdge
					+ "|";

			ArrayList<Long> inGoingEdges = newNode.getInGoingEdges();

			for (Long id : inGoingEdges) {
				nodeWithUpdatedPR = nodeWithUpdatedPR + id + " ";
			}
			if (inGoingEdges.size() > 0){
				nodeWithUpdatedPR = nodeWithUpdatedPR.substring(0, nodeWithUpdatedPR.length() - 1) + "|";
			}
			else{
				nodeWithUpdatedPR += " |";
			}

			ArrayList<Long> boundaryV = newNode.getBoundaryVertices();

			for (Long id : boundaryV) {
				nodeWithUpdatedPR = nodeWithUpdatedPR + id + " ";
			}
			if (boundaryV.size() > 0){
				nodeWithUpdatedPR = nodeWithUpdatedPR.substring(0, nodeWithUpdatedPR.length() - 1) + "|";
			}
			else{
				nodeWithUpdatedPR += " |";
			}
			
			netResidualError += (initialPageRank - newPageRank)/newPageRank;
			output.collect(new LongWritable(nodeID), new Text(nodeWithUpdatedPR));
		}
		
		reporter.incrCounter(PageRank.MyCounters.RESIDUAL_ERROR, (long)(netResidualError * 1000));
		reporter.incrCounter(PageRank.MyCounters.NUM_ITERATIONS, (long) numIterations);
//		System.out.println("BLOCK " + arg0 + " had residual error " + (long)(netResidualError * 1000));
		
		

	}

}
