package de.bigdprak.ss2016;

/* SimpleApp.java */
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkSimpleApp {
	public static void main(String[] args) {
		String logFile = "/home/balthorius/progs/spark/eclipse_log.txt";
		//String logFile = "~/Schreibtisch/spark-1.6.1/README.md"; // Should be
																	// some file
																	// on your
																	// system
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		//conf.setMaster("spark://wdi06.informatik.uni-leipzig.de:7077");
		//conf.setMaster("spark://master:7077");
		//conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

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

		System.out.println("Lines with a: " + numAs + ", lines with b: "
				+ numBs);
		
		// ---------------------------------------------------------------------

		JavaRDD<String> textFile = sc.textFile("hdfs://...");
		JavaRDD<String> words = textFile
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) {
						return Arrays.asList(s.split(" "));
					}
				});
		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});
		JavaPairRDD<String, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});
		counts.saveAsTextFile("hdfs://...");
		sc.close();

	}
}
