package de.bigdprak.ss2016;

/* SimpleApp.java */
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {

    public static void main( String[] args ) throws IOException, URISyntaxException
    {


        Configuration conf = new Configuration();
        FileSystem fs = new DistributedFileSystem();
        fs.initialize(new URI("hdfs://bigprak@wdi06.informatik.uni-leipzig.de:6069"), conf);


        for (FileStatus f :fs.listStatus(new Path("/")))
        {
            System.out.println(f.getPath().getName());                  
        }

        fs.close();

    }
	
	
	/*	public static void main(String[] args) {
		String logFile = "/home/richard/logfiles/SimpleApp_log.txt"; // Should be some file on your system
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> logData = sc.textFile(logFile).cache();
		
	    JavaSparkContext sc = new JavaSparkContext("hdfs://wdi06.informatik.uni-leipzig.de:8020", "Simple App", "/home/richard/Schreibtisch/spark-1.6.1", new String[]{"target/geo-vis-1.0-SNAPSHOT.jar"});
	    JavaRDD<String> logData = sc.textFile(logFile).cache();
		
		
		
		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("a"); }
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("b"); }
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}

*/


}
