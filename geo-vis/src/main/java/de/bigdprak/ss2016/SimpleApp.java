package de.bigdprak.ss2016;

/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args) {

		String master;
		String input_file;

		//input_file = "/home/balthorius/progs/hadoop/input/Affiliations.txt";
		master = "local";

		input_file = "/home/bigprak/progs/hadoop/input/Affiliations.txt";
		
		// input_file = "hdfs:///users/bigprak/input/Affiliations.txt";
		//master = "spark://wdi06.informatik.uni-leipzig.de:7077";
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// JavaRDD<String> logData = sc.textFile(logFile).cache();
		JavaRDD<String> logData = sc.textFile(input_file).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}
}