package de.bigdprak.ss2016;

/* SimpleApp.java */
import java.io.File;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import de.bigdprak.ss2016.database.Author;

public class SimpleApp {
	
	private static String master = "local";
	
	@SuppressWarnings("serial")
	public static int getLineCount(String input_file) {

		// input_file = "hdfs:///users/bigprak/input/Affiliations.txt";
		//master = "spark://wdi06.informatik.uni-leipzig.de:7077";
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster(master);
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		JavaSparkContext sc = new JavaSparkContext(conf);
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
		//class GetLength implements Function<String, Integer> {
		//	public Integer call(String s) {
		//		return s.length();
		//	}
		//};
		
		class Sum implements Function2<Integer, Integer, Integer> {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		}
		
		// mapping the length of each line to that line
		//System.out.println("Do the mapping...");
		//JavaRDD<Integer> lineLengths = lines.map(new GetLength());
		// reducing all line lengths to the aggregated sum
		//System.out.println("Do the reduce job...");
		//int totalLength = lineLengths.reduce(new Sum());
		//System.out.println("total length: " + totalLength);
		
		// compute linecount
		JavaRDD<Integer> one_per_line = lines.map(new Function<String, Integer>() {
			public Integer call(String s) {
				return 1;
			}
		});
		int linecount = one_per_line.reduce(new Sum());
		System.out.println("Dokument: " + input_file);
		System.out.println("Zeilenanzahl: " + linecount);
		
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
		
		return linecount;
	}
	
	@SuppressWarnings("serial")
	public static List<String> sql_getAuthorNames(String input_path) {
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster(master);
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<Author> authors = sc.textFile(input_path).map(
			new Function<String, Author>() {
				public Author call(String line) throws Exception {
					String[] parts = line.split("\t");
					return new Author(parts[1]);
				}
			}
		);
		DataFrame schemaAuthor = sqlContext.createDataFrame(authors, Author.class);
		schemaAuthor.registerTempTable("authors");
		
		DataFrame content = sqlContext.sql("select name from authors");
		List<String> authorNames = content.javaRDD().map(
			new Function<Row, String>() {
				public String call(Row row) {
					return row.getString(0);
				}
			}
		).collect();
		
		sc.close();
		
		return authorNames;
	}
	
	public static void main(String[] args) {
		
		boolean isRemote = (args.length > 0);
		
		String path;
		path = isRemote ? "/home/bigprak/progs/hadoop/input"
						: "/home/balthorius/progs/hadoop/input/";
		//path = "hdfs:///users/bigprak/input/Affiliations.txt";
		File directory = new File(path);
		File[] files = directory.listFiles();
		String[] out = new String[files.length]; 
		
		boolean countLines = false;
		if (countLines) {
			// compute linecount for each file
			for (int i = 0; i < files.length; i++) {
				String p = files[i].getAbsolutePath();
				int linecount = SimpleApp.getLineCount(p);
				//int linecount = 1;
				out[i] = new String(p + ":\t" + linecount);
			}
		}
		
		boolean compute = true;
		if (compute) {
			path = isRemote ? "/home/bigprak/progs/hadoop/input/Authors.txt"
							: "/home/balthorius/progs/hadoop/input/Authors.txt";
			List<String> authors = SimpleApp.sql_getAuthorNames(path);
			int i = 0;
			for (String s : authors) {
				i++;
				System.out.println(i + "\t" + s);
			}
		}
		
		// output
//		for (String s: out) {
//			System.out.println(s);
//		}
		
	}
}