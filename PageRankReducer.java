package classes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements
		Reducer<LongWritable, Node, LongWritable, LongWritable> {

	@Override
	public void reduce(LongWritable arg0, Iterator<Node> arg1,
			OutputCollector<LongWritable, LongWritable> output, Reporter arg3)
			throws IOException {

		// Stores all the nodes that have been given to this reducer
		HashMap<Long, Node> allNodes = new HashMap<Long, Node>();

		ArrayList<Long> blockNodes = new ArrayList<Long>();
		// Stores all the pageranks of the nodes
		HashMap<Long, Long> newNodePageRanks = new HashMap<Long, Long>();

		while (arg1.hasNext()) {
			Node currentNode = arg1.next();
			if (currentNode.getblockID() == arg0.get()) {
				// belongs to the block
				blockNodes.add(currentNode.getNodeID());
				newNodePageRanks.put(currentNode.getNodeID(), new Long(0));
			}
			allNodes.put(currentNode.getNodeID(), currentNode);
		}

		HashMap<Long, Long> initialPageRanks = new HashMap<Long, Long>();
		for (Long nodeID : blockNodes) {
			initialPageRanks.put(nodeID, allNodes.get(nodeID).getPageRank());
		}

		Long residualError = new Long(0);
		int numIterations = 0;

		//also include the constraint of number of iterations
		while (residualError >= 0.001) {
			numIterations++;
			for (Long nodeID : blockNodes) {
				Node currentNode = allNodes.get(nodeID);
				Long newPageRank = new Long(0);
				for (Long inGoingEdgeID : currentNode.getInGoingEdges()) {
					Node inGoingNode = allNodes.get(inGoingEdgeID);
					newPageRank += inGoingNode.getPageRank() / inGoingNode.getNumOutEdges();
				}
				newNodePageRanks.put(nodeID, newPageRank + (long) ((1 - 0.85) / allNodes.size()));
				residualError += (allNodes.get(nodeID).getPageRank() - newPageRank)/newPageRank;
			}
			for(Long nodeID: blockNodes){
				allNodes.get(nodeID).setPageRank(newNodePageRanks.get(nodeID));
			}
		}
		
		Long netResidualError = new Long(0);
		for (Long nodeID: initialPageRanks.keySet()){
			Long newPageRank = allNodes.get(nodeID).getPageRank();
			Long initialPageRank = initialPageRanks.get(nodeID);
			netResidualError += (initialPageRank - newPageRank)/newPageRank;
			output.collect(new LongWritable(nodeID), new LongWritable(newPageRank));
		}
		
		

	}

}
