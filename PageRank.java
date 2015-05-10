package classes;

// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

//Main class that sets up which classes are the mappers and which classes are the reducers
public class PageRank {
	
	public static ArrayList<Integer> blockings = generateBlockGroupings();

	// args represents the paths of the input file and output file
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: PageRank <input path> <output path>");
			System.exit(-1);
		}

		// data preprocessing
		// change edges.txt and blocks.txt into nodes.txt

		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("PageRank");

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		JobClient.runJob(conf);
	}

	public static ArrayList<Integer> generateBlockGroupings() {
		ArrayList<Integer> blockID = new ArrayList<Integer>();
		blockID.add(10328);
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

	public static Long blockIDofNode(long nodeID){
		return blockIDofNode(nodeID, blockings.size()/2, blockings.size()/4);
	}
	
	public static Long blockIDofNode(long nodeID, int index, int margin) {

		if (margin == 0) margin = 1;
		else { margin = (int) Math.floor(margin / 2);}

		if (index == 0 || index == blockings.size() || (nodeID <= blockings.get(index) && nodeID > blockings
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

}
