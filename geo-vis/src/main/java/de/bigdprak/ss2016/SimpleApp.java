package de.bigdprak.ss2016;

/* SimpleApp.java */
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SimpleApp {
	@SuppressWarnings("serial")
	public static void main(String[] args) {

		String master;
		String input_file;

		//input_file = "/home/balthorius/progs/hadoop/input/Affiliations.txt";
		input_file = "/home/balthorius/progs/hadoop/input/PaperAuthorAffiliations.txt";
		master = "local";

		//input_file = "/home/bigprak/progs/hadoop/input/Affiliations.txt";
		
		// input_file = "hdfs:///users/bigprak/input/Affiliations.txt";
		//master = "spark://wdi06.informatik.uni-leipzig.de:7077";
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster(master);
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		JavaSparkContext sc = new JavaSparkContext(conf);
		// JavaRDD<String> logData = sc.textFile(logFile).cache();
		JavaRDD<String> logData = sc.textFile(input_file);//.cache();
		
		JavaRDD<String> lines = sc.textFile(input_file);
		
		// *** Version 1 ***
		//JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		//int totalLength = lineLengths.reduce((a,b) -> a + b);
		
		// *** Version 2 ***
		//JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
		//	public Integer call(String s) { return s.length(); }
		//});
		//int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
		//	public Integer call(Integer a, Integer b) { return a + b; }
		//});
		
		// *** Version 3 ***
		class GetLength implements Function<String, Integer> {
			public Integer call(String s) {
				return s.length();
			}
		};
		class Sum implements Function2<Integer, Integer, Integer> {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		}
		
		// mapping the length of each line to that line
		System.out.println("Do the mapping...");
		JavaRDD<Integer> lineLengths = lines.map(new GetLength());
		// reducing all line lengths to the aggregated sum
		System.out.println("Do the reduce job...");
		int totalLength = lineLengths.reduce(new Sum());
		System.out.println("total length: " + totalLength);
		
		/*
		System.out.println("Do the filter a job...");
		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		System.out.println("Do the filter b job...");
		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		*/
		sc.close();
	}
}