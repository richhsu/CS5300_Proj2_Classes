package classes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
public class NodeWritable implements Writable{
	
	private float pageRank; // 2
	private long nodeID; // 1
	private long blockID; // 3
	private int numOutEdges; // 4
	private ArrayList<Long> inGoingEdges; // 5
	private ArrayList<Long> boundaryVertices; // 6
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		nodeID = arg0.readLong();
		pageRank = arg0.readFloat();
		blockID = arg0.readLong();
		numOutEdges = arg0.readInt();
		Integer numInEdges = arg0.readInt();
		inGoingEdges = new ArrayList<Long>();
		for(int i = 0; i < numInEdges; i++){
			inGoingEdges.add(arg0.readLong());
		}
		Integer numBoundaryVertices = arg0.readInt();
		boundaryVertices = new ArrayList<Long>();
		for(int j = 0; j < numBoundaryVertices; j++){
			boundaryVertices.add(arg0.readLong());
		}
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(nodeID);
		arg0.writeFloat(pageRank);
		arg0.writeLong(blockID);
		arg0.writeInt(numOutEdges);
		arg0.writeInt(inGoingEdges.size());
		for(Long inID: inGoingEdges){
			arg0.writeLong(inID);
		}
		arg0.writeInt(boundaryVertices.size());
		for(Long bV: boundaryVertices){
			arg0.writeLong(bV);
		}
	}
	
	public static NodeWritable read(DataInput in) throws IOException{
		NodeWritable n = new NodeWritable();
		n.readFields(in);
		return n;
	}
	
	public NodeWritable(float pR, long nID, long bID, int nOutEdges, ArrayList<Long> iEdges, ArrayList<Long> bVertices){
		pageRank = pR;
		nodeID = nID;
		blockID = bID;
		numOutEdges = nOutEdges;
		inGoingEdges = iEdges;
		boundaryVertices = bVertices;
	}
	
	public NodeWritable(){
		
	}
	
	public float getPageRank(){
		return pageRank;
	}
	
	public void setPageRank(Float pR){
		pageRank = pR;
	}
	
	public long getNodeID(){
		return nodeID;
	}
	
	public long getblockID(){
		return blockID;
	}
	
	public int getNumOutEdges(){
		return numOutEdges;
	}
	
	public void setNumOutEdges(int outEdges){
		numOutEdges = outEdges;
	}
	
	public ArrayList<Long> getInGoingEdges(){
		return inGoingEdges;
	}
	
	public ArrayList<Long> getBoundaryVertices(){
		return boundaryVertices;
	}
	
	public void print(){
		System.out.println("Node " + nodeID);
//		System.out.println("PageRank " + pageRank);
//		System.out.println("BlockID " + blockID);
		for(Long in: inGoingEdges){
			System.out.println("IngoingEdge " + in);
		}
		for(Long b: boundaryVertices){
			System.out.println("Boundary Vertex " + b);
		}
	}
	
	public Node createNode(){
		return new Node(pageRank, nodeID, blockID, numOutEdges, inGoingEdges, boundaryVertices);
	}
}