package classes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Node implements Writable{
	
	private long pageRank;
	private long nodeID;
	private long blockID;
	private int numOutEdges;
	private ArrayList<Long> inGoingEdges;
	private ArrayList<Long> boundaryVertices;
	
	public long getPageRank(){
		return pageRank;
	}
	
	public void setPageRank(Long pR){
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
	
	public ArrayList<Long> getInGoingEdges(){
		return inGoingEdges;
	}
	
	public ArrayList<Long> getBoundaryVertices(){
		return boundaryVertices;
	}
	

	@Override
	public void readFields(DataInput arg0) throws IOException {
		nodeID = arg0.readLong();
		pageRank = arg0.readLong();
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
		arg0.writeLong(pageRank);
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
	
	public static Node read(DataInput in) throws IOException{
		Node n = new Node();
		n.readFields(in);
		return n;
	}
	
	public Node(long pR, int nID, int bID, int nOutEdges, ArrayList<Long> iEdges, ArrayList<Long> bVertices){
		pageRank = pR;
		nodeID = nID;
		blockID = bID;
		numOutEdges = nOutEdges;
		inGoingEdges = iEdges;
		boundaryVertices = bVertices;
	}
	
	public Node(){
		
	}

}
