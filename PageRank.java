package classes;

// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;



//Main class that sets up which classes are the mappers and which classes are the reducers
public class PageRank {

	// compute filter parameters for netid bce25
	public static final double fromNetID = 0.52; // 52 is 25 reversed
	public static final double rejectMin = 0.9 * fromNetID;
	public static final double rejectLimit = rejectMin + 0.01;

	// Stores all the nodes in the entire graph
	public static HashMap<Long, Node> nodes = new HashMap<Long, Node>();

	// Key value pairs of blockID (long) to List of nodes in that block
	public static HashMap<Long, List<Long>> blocks = new HashMap<Long, List<Long>>();

	public static ArrayList<Long> blockings = generateBlockGroupings();

	protected static enum MyCounters {
		RESIDUAL_ERROR, NUM_ITERATIONS
	}

	// args represents the paths of the input file and output file
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: PageRank <input path> <output path>");
			System.exit(-1);
		}

		dataPreprocessing();
		System.out.println("Done preprocessing");

		// *******************************************************************
		// Constructing the mapreduce job

		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("PageRank");

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);

		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(NodeWritable.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(FloatWritable.class);
		int numRounds = 0;
		while (true) {
			numRounds++;
			RunningJob r = JobClient.runJob(conf);
			Counters c = r.getCounters();
			long rError = c.getCounter(MyCounters.RESIDUAL_ERROR);
			long totalIterations = c.getCounter(MyCounters.NUM_ITERATIONS);
			System.out.println("average number of iterations " + (totalIterations / 68.0));
			System.out.println("error is " + rError);
			if((Math.abs(rError) < (100)) || numRounds > 3){
				System.out.println(rError);
				break;
			}
			updateNodesTxt();
		}
	}
	
	public static void updateNodesTxt(){
		System.out.println("updating previous nodes file");
		File previousNodes = new File("./input/nodes.txt");
		deleteFile(previousNodes);
		File newNodes = new File("./output/part-00000");
		newNodes.renameTo(new File("./input/nodes.txt"));
		deleteFile(new File("./output"));
	}
	
	public static void deleteFile(File f){
		if(!f.exists()) return;
		if(f.isDirectory()){
			for(File subFile: f.listFiles()){
				deleteFile(subFile);
			}
		}
		f.delete();
	}

	public static void dataPreprocessing() throws NumberFormatException,
			IOException {

		BufferedReader fromEdges = new BufferedReader(new FileReader(
				"./givenData/edges2.txt"));
		String inLine;
		String delims = "[ ]+";
		int numberOfNodes = 685230;
		while ((inLine = fromEdges.readLine()) != null) {
			String[] tokens = inLine.split(delims);
			double floating = Double.parseDouble(tokens[1]);
			long source = Long.parseLong(tokens[2]);
			long destination = Long.parseLong(tokens[3]);

			if (selectInputLine(floating)) {

				if (nodes.get(source) == null) {
					Node create = initializeNode(source, numberOfNodes);
					nodes.put(source, create);

				}

				long destinationBlockID = blockIDofNode(destination,
						blockings.size() / 2, blockings.size() / 4);

				if (destinationBlockID != nodes.get(source).getblockID()) {
					nodes.get(source).getBoundaryVertices().add(destination);
				}

				nodes.get(source).setNumOutEdges(
						nodes.get(source).getNumOutEdges() + 1);

				if (nodes.get(destination) == null) {
					Node create = initializeNode(destination, numberOfNodes);
					nodes.put(destination, create);
				}

				nodes.get(destination).getInGoingEdges().add(source);

			}
		}
		fromEdges.close();

		writeToNodestxt(nodes);

	}

	public static boolean selectInputLine(double x) {
		return (((x >= rejectMin) && (x < rejectLimit)) ? false : true);
	}

	public static Node initializeNode(long nodeID, int numberOfNodes) {
		float pagerank = 1 / (float) (numberOfNodes);
		long nID = nodeID;
		long bID = blockIDofNode(nodeID);
		int nOutEdges = 0;
		ArrayList<Long> iEdges = new ArrayList<Long>();
		ArrayList<Long> bVertices = new ArrayList<Long>();
		Node create = new Node(pagerank, nID, bID, nOutEdges, iEdges, bVertices);

		return create;
	}

	public static ArrayList<Long> generateBlockGroupings() {
		ArrayList<Long> blockID = new ArrayList<Long>();
		blockID.add(new Long(10328));
		blockID.add(blockID.get(0) + 10045);
		blockID.add(blockID.get(1) + 10256);
		blockID.add(blockID.get(2) + 10016);
		blockID.add(blockID.get(3) + 9817);
		blockID.add(blockID.get(4) + 10379);
		blockID.add(blockID.get(5) + 9750);
		blockID.add(blockID.get(6) + 9527);
		blockID.add(blockID.get(7) + 10379);
		blockID.add(blockID.get(8) + 10004);
		blockID.add(blockID.get(9) + 10066);
		blockID.add(blockID.get(10) + 10378);
		blockID.add(blockID.get(11) + 10054);
		blockID.add(blockID.get(12) + 9575);
		blockID.add(blockID.get(13) + 10379);
		blockID.add(blockID.get(14) + 10379);
		blockID.add(blockID.get(15) + 9822);
		blockID.add(blockID.get(16) + 10360);
		blockID.add(blockID.get(17) + 10111);
		blockID.add(blockID.get(18) + 10379);
		blockID.add(blockID.get(19) + 10379);
		blockID.add(blockID.get(20) + 10379);
		blockID.add(blockID.get(21) + 9831);
		blockID.add(blockID.get(22) + 10285);
		blockID.add(blockID.get(23) + 10060);
		blockID.add(blockID.get(24) + 10211);
		blockID.add(blockID.get(25) + 10061);
		blockID.add(blockID.get(26) + 10263);
		blockID.add(blockID.get(27) + 9782);
		blockID.add(blockID.get(28) + 9788);
		blockID.add(blockID.get(29) + 10327);
		blockID.add(blockID.get(30) + 10152);
		blockID.add(blockID.get(31) + 10361);
		blockID.add(blockID.get(32) + 9780);
		blockID.add(blockID.get(33) + 9982);
		blockID.add(blockID.get(34) + 10284);
		blockID.add(blockID.get(35) + 10307);
		blockID.add(blockID.get(36) + 10318);
		blockID.add(blockID.get(37) + 10375);
		blockID.add(blockID.get(38) + 9783);
		blockID.add(blockID.get(39) + 9905);
		blockID.add(blockID.get(40) + 10130);
		blockID.add(blockID.get(41) + 9960);
		blockID.add(blockID.get(42) + 9782);
		blockID.add(blockID.get(43) + 9796);
		blockID.add(blockID.get(44) + 10113);
		blockID.add(blockID.get(45) + 9798);
		blockID.add(blockID.get(46) + 9854);
		blockID.add(blockID.get(47) + 9918);
		blockID.add(blockID.get(48) + 9784);
		blockID.add(blockID.get(49) + 10379);
		blockID.add(blockID.get(50) + 10379);
		blockID.add(blockID.get(51) + 10199);
		blockID.add(blockID.get(52) + 10379);
		blockID.add(blockID.get(53) + 10379);
		blockID.add(blockID.get(54) + 10379);
		blockID.add(blockID.get(55) + 10379);
		blockID.add(blockID.get(56) + 10379);
		blockID.add(blockID.get(57) + 9981);
		blockID.add(blockID.get(58) + 9782);
		blockID.add(blockID.get(59) + 9781);
		blockID.add(blockID.get(60) + 10300);
		blockID.add(blockID.get(61) + 9792);
		blockID.add(blockID.get(62) + 9782);
		blockID.add(blockID.get(63) + 9782);
		blockID.add(blockID.get(64) + 9862);
		blockID.add(blockID.get(65) + 9782);
		blockID.add(blockID.get(66) + 9782);

		return blockID;
	}

	public static Long blockIDofNode(long nodeID) {
		return blockIDofNode(nodeID, blockings.size() / 2, blockings.size() / 4);
	}

	public static Long blockIDofNode(long nodeID, int index, int margin) {

		if (margin == 0)
			margin = 1;
		else {
			margin = (int) Math.floor(margin / 2);
		}

		if (index == 0
				|| index == blockings.size()
				|| (nodeID <= blockings.get(index) && nodeID > blockings
						.get(index - 1))) {
			return (long) (index);
		}

		if (nodeID > blockings.get(index)) {
			int newIndex = index + margin;
			return blockIDofNode(nodeID, newIndex, margin);
		} else {
			int newIndex = index - margin;
			return blockIDofNode(nodeID, newIndex, margin);
		}

	}

	public static void writeToNodestxt(HashMap<Long, Node> mapOfNodes)
			throws FileNotFoundException {
		PrintWriter out = new PrintWriter("./input/nodes.txt");

		for (Node n : mapOfNodes.values()) {
			long nID = n.getNodeID();
			float pr = n.getPageRank();
			long bID = n.getblockID();
			int numOutEdge = n.getNumOutEdges();

			String outLine = nID + "	" + pr + "|" + bID + "|" + numOutEdge
					+ "|";

			ArrayList<Long> inGoingEdges = n.getInGoingEdges();

			for (Long id : inGoingEdges) {
				outLine = outLine + id + " ";
			}
			if (inGoingEdges.size() > 0) {
				outLine = outLine.substring(0, outLine.length() - 1) + "|";
			} else {
				outLine += " |";
			}

			ArrayList<Long> boundaryV = n.getBoundaryVertices();

			for (Long id : boundaryV) {
				outLine = outLine + id + " ";
			}
			if (boundaryV.size() > 0) {
				outLine = outLine.substring(0, outLine.length() - 1) + "|";
			} else {
				outLine += " |";
			}

			out.println(outLine);
		}
		out.close();
	}

	public static void readFromNodestxt() throws NumberFormatException,
			IOException {

		BufferedReader fromNodes = new BufferedReader(new FileReader(
				"./input/nodes.txt"));

		String inLine;
		String attributeDelim = "[|]";
		String listDelim = "[ ]";

		while ((inLine = fromNodes.readLine()) != null) {
			String[] tokens = inLine.split(attributeDelim);

			long nodeID = Long.parseLong(tokens[0]);
			float pagerank = Float.parseFloat(tokens[1]);
			long blockID = Long.parseLong(tokens[2]);
			int numOut = Integer.parseInt(tokens[3]);
			String[] inEdges = tokens[4].split(listDelim);
			String[] boundaryEdges = tokens[5].split(listDelim);

		}
		fromNodes.close();
	}

}
