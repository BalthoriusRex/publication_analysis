package de.bigdprak.ss2016;

/* SimpleApp.java */
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
import org.apache.spark.sql.types.StructType;

import de.bigdprak.ss2016.database.*;
import de.bigdprak.ss2016.utils.UTF8Writer;

public class SimpleApp {
	
	private static String master = "local";
	//private static String file_2016KDDCupSelectedAffiliations;
	//private static String file_2016KDDCupSelectedPapers;
	private static String file_Authors;
	private static String file_Affiliations;
	private static String file_ConferenceInstances;
	private static String file_Conferences;
	private static String file_FieldOfStudyHierarchy;
	private static String file_FieldsOfStudy;
	private static String file_Journals;
	private static String file_PaperAuthorAffiliations;
	private static String file_PaperKeywords;
	private static String file_PaperReferences;
	private static String file_Papers;
	private static String file_PaperUrls;
	private static String[] files;
	
	public static void setFileNames(String folder) {

		file_Authors = folder + "Authors.txt";
		file_Affiliations = folder + "Affiliations.txt";
		file_ConferenceInstances = folder + "ConferenceInstances.txt";
		file_Conferences = folder + "Conferences.txt";
		file_FieldOfStudyHierarchy = folder + "FieldOfStudyHierarchy.txt";
		file_FieldsOfStudy = folder + "FieldsOfStudy.txt";
		file_Journals = folder + "Journals.txt";
		file_PaperAuthorAffiliations = folder + "PaperAuthorAffiliations.txt";
		file_PaperKeywords = folder + "PaperKeywords.txt";
		file_PaperReferences = folder + "PaperReferences.txt";
		file_Papers = folder + "Papers.txt";
		file_PaperUrls = folder + "PaperUrls.txt";
		
		files = new String[] {
			file_Authors,
			file_Affiliations,
			file_ConferenceInstances,
			file_Conferences,
			file_FieldOfStudyHierarchy,
			file_FieldsOfStudy,
			file_Journals,
			file_PaperAuthorAffiliations,
			file_PaperKeywords,
			file_PaperReferences,
			file_Papers,
			file_PaperUrls
		};
	}
	
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
					return new Author(parts[0], parts[1]);
				}
			}
		);
		DataFrame schemaAuthor = sqlContext.createDataFrame(authors, Author.class);
		schemaAuthor.registerTempTable("authors");
		
		DataFrame content = sqlContext.sql("select name from authors where name like 'n%'");
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
	
	@SuppressWarnings("serial")
	public static void sql_getPaperAuthorAffiliations(String input_path) {
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster(master);
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<PaperAuthorAffiliation> paperAuthorAffiliations = sc.textFile(input_path).map(
			new Function<String, PaperAuthorAffiliation>() {
				public PaperAuthorAffiliation call(String line) throws Exception {
					String[] parts = line.split("\t");
					return new PaperAuthorAffiliation(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
//					String[] input = new String[6];
//					int i = 0;
//					for (String s: parts) {
//						if (!(s.isEmpty())) {
//							input[i] = s;
//							i++;
//						}
//					}
//					if (i == 6) {
//						return new PaperAuthorAffiliation(input[0], input[1], input[2], input[3], input[4], input[5]);
//					} else {
//						System.out.println("DEBUG : " + line);
//						return new PaperAuthorAffiliation(-1, -1, -1, "A", "B", -1);
//					}
				}
			}
		);
		DataFrame schema = sqlContext.createDataFrame(paperAuthorAffiliations, PaperAuthorAffiliation.class);
		schema.registerTempTable("paperAuthorAffiliations");
		
		DataFrame content = sqlContext.sql(""
				+ "select paperID, count(authorID) "
				+ "from paperAuthorAffiliations "
				+ "group by paperID "
				+ "order by count(authorID) desc "
				//+ "limit by 5"
				);
		
		int result = content.javaRDD().map(
			new Function<Row, Integer>() {
				public Integer call(Row row) {
					int ret = 0;
					if (row != null)	{
						System.out.println(row.toString());
					} else {
						ret = 1;
					}
					return ret;
				}
			}
		).reduce(
			new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer a, Integer b) {
					return a+b;
				}
			}
		);
		System.out.println("counted " + result + " null-lines...");
		
		/*
		paperID, count(authorID)
		[2145616859,8]
		[2126583859,8]
		[2051465859,8]
		[1976245259,8]
		[1675078859,8]
		[1965067459,8]
		[2044884259,8]
		[1542241859,8]
		[2087617859,8]
		[2018584859,8]
		[1592316859,8]
		[2144967059,8]
		[92825459,8]
		[1988342459,8]
		[2181527955,8]
		[1543565459,8]
		[2004787659,8]
		[2004428859,8]
		[2233864555,8]
		[2155723555,8]
		[2125824059,8]
		[2251348755,8]
		[2055464459,8]
		[2053106459,8]
		[1997459259,8]
		[564843259,8]
		[1533104059,8]
		[2121517459,8]
		[2124518659,8]
		[997032859,8]
		[2079736059,8]
		[1763095659,8]
		[1979935059,8]
		[1600059459,8]
		[2181843155,8]
		[1834513459,8]
		[1574607659,8]
		[1742108059,8]
		[372374459,8]
		[2089457459,8]
		[63112459,8]
		[762084659,8]

		*/
		sc.close();
	}
	
	@SuppressWarnings("serial")
	private static void buildTables(JavaSparkContext sc, SQLContext sqlContext) {
		/*
		// MUSTER
		// create Table Affiliation
		JavaRDD<Affiliation> affiliations = sc.textFile(file_Affiliations).map(
			new Function<String, Affiliation>() {
				public Affiliation call(String line) throws Exception {
					String[] parts = line.split("\t");
					return new Affiliation(parts);
				}
			}
		);
		DataFrame schemaAffiliations = sqlContext.createDataFrame(affiliations, Affiliation.class);
		schemaAffiliations.registerTempTable("Affiliation");
		*/
		
		// create Table Affiliation
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_Affiliations).map(
				new Function<String, Affiliation>() {
					public Affiliation call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new Affiliation(parts);
					}
				}
			),
			Affiliation.class)
		.registerTempTable("Affiliation");
		
		// create Table Author
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_Authors).map(
				new Function<String, Author>() {
					public Author call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new Author(parts);
					}
				}
			),
			Author.class)
		.registerTempTable("Author");
		
		// create Table PaperAuthorAffiliation
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_PaperAuthorAffiliations).map(
				new Function<String, PaperAuthorAffiliation>() {
					public PaperAuthorAffiliation call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new PaperAuthorAffiliation(parts);
					}
				}
			),
			PaperAuthorAffiliation.class)
		.registerTempTable("PaperAuthorAffiliation");
		
		// create Table Paper
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_Papers).map(
				new Function<String, Paper>() {
					public Paper call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new Paper(parts);
					}
				}
			),
			Paper.class)
		.registerTempTable("Paper");
		
		/*
		// create Table Author
		JavaRDD<Author> authors = sc.textFile(file_Authors).map(
			new Function<String, Author>() {
				public Author call(String line) throws Exception {
					String[] parts = line.split("\t");
					return new Author(parts);
				}
			}
		);
		DataFrame schemaAuthor = sqlContext.createDataFrame(authors, Author.class);
		schemaAuthor.registerTempTable("Author");
		
		// create Table PaperAuthorAffiliation
		JavaRDD<PaperAuthorAffiliation> paas = sc.textFile(file_PaperAuthorAffiliations).map(
			new Function<String, PaperAuthorAffiliation>() {
				public PaperAuthorAffiliation call(String line) throws Exception {
					String[] parts = line.split("\t");
					return new PaperAuthorAffiliation(parts);
				}
			}
		);
		DataFrame schemaPaa = sqlContext.createDataFrame(paas, PaperAuthorAffiliation.class);
		schemaPaa.registerTempTable("PaperAuthorAffiliation");
		
		// create Table Paper
		JavaRDD<Paper> papers = sc.textFile(file_Papers).map(
			new Function<String, Paper>() {
				public Paper call(String line) throws Exception {
					String[] parts = line.split("\t");
					return new Paper(parts);
				}
			}
		);
		DataFrame schemaPapers = sqlContext.createDataFrame(papers, Paper.class);
		schemaPapers.registerTempTable("Paper");
		*/
				
	}
	
	@SuppressWarnings("serial")
	public static List<Row> sql_answerQuery(String query) {
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster(master);
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		buildTables(sc, sqlContext);
		
		
		DataFrame content = sqlContext.sql(query);
		List<Row> result = content.javaRDD().map(
			new Function<Row, Row>() {
				public Row call(Row row) {
					//System.out.println(" " + row.schema().toString());
					//String out = row.toString();
					//System.out.println("\t" + out);
					//return out;
					return row;
				}
			}
		).collect();
		
		sc.close();
		
		return result;
	}
	
	public static void main(String[] args) {
		
		boolean isRemote = false;
		if (args.length > 1) {
			isRemote = args[1].equalsIgnoreCase("remote");
		}
		
		System.out.println("Working on " + (isRemote ? "remote PC" : "local PC" + "!"));
		
		String user = isRemote ? "bigprak" : "balthorius"; 
		String folder = "/home/"+user+"/progs/hadoop/input/";
		setFileNames(folder);
		
		//path = "hdfs:///users/bigprak/input/Affiliations.txt";
		
		String[] out = new String[files.length]; 
		
		boolean countLines = false;
		if (countLines) {
			// compute linecount for each file
			for (int i = 0; i < files.length; i++) {
				String p = files[i];
				int linecount = SimpleApp.getLineCount(p);
				//int linecount = 1;
				out[i] = new String(p + ":\t" + linecount);
			}
		}
		
		boolean compute;
		
		compute = false;
		if (compute) {
			List<String> authors = SimpleApp.sql_getAuthorNames(file_Authors);
			int i = 0;
			System.out.println("Output [");
			for (String s : authors) {
				i++;
				System.out.println(i + "\t" + s);
			}
			System.out.println("]");
		}
		
		compute = false;
		if (compute) {
			sql_getPaperAuthorAffiliations(file_PaperAuthorAffiliations);
		}
		
		// Liste alle Autoren des Papers paperID mit entsprechenden Afiliations auf
		compute = false;
		if (compute) {
			long paperID = 2145616859;
			String query = ""
					+ "SELECT p.originalPaperTitle, paa.authorSequenceNumber, a.name, paa.originalAffiliationName "
					+ "FROM PaperAuthorAffiliation paa, Author a, Paper p "
					//+ "FROM "
					+ "WHERE "
					+          "paa.paperID = " + paperID + " "
					+ "AND " + "paa.authorID = a.authorID "
					+ "AND " + "paa.paperID = p.paperID "
					+ "ORDER BY paa.authorSequenceNumber ASC";
			List<Row> result = sql_answerQuery(query);
			for (Row row: result) {
				System.out.println(row.toString());
			}
		}
		
		// Welche Affiliations gibt es?
		compute = true;
		if (compute) {
			String query = ""
					+ "SELECT count(normalizedAffiliationName) as Anzahl, normalizedAffiliationName as Name "
					+ "FROM PaperAuthorAffiliation "
					//+ "WHERE NOT (normalizedAffiliationName = '') "
					+ "GROUP BY normalizedAffiliationName "
					+ "ORDER BY normalizedAffiliationName ASC"
					;
			String outfile = folder + "/output.txt";
//			TextFileWriter.writeOver(outfile, query);
//			TextFileWriter.writeOn(outfile, "\n\n\n");
//			TextFileWriter.writeOn(outfile, query);
			List<Row> result = sql_answerQuery(query);
			
			UTF8Writer writer = new UTF8Writer(outfile);
			writer.clear();
			writer.append(""
					+ "<Document>\n"
					+ "	<Folder>\n");
			for (Row row: result) {
				
				/*
				 * 
				 * <Placemark>
				 * 		<affiliation>...</affiliation>
				 * 		<anzahl>...</anzahl>
				 * 
				 * 
				 */

				String s = row.toString();
				String[] parts = s.substring(1, s.length()-1)
								  .split(",");
				System.out.println("DEBUG : " + s);
				String anzahl = parts[0];
				String affiliation;
				try {
					affiliation = parts[1];
				} catch (ArrayIndexOutOfBoundsException e) {
					System.err.println("[ERROR] empty affiliation name");
					affiliation = "";
				}
				
				String newS = ""
						+ "		<Placemark>\n"
						+ "			<affiliation>" + affiliation + "</affiliation>\n"
						+ "			<anzahl>" + anzahl + "</anzahl>\n"
						+ "		</Placemark>\n";
				
				writer.append(newS);
			}
			writer.append(""
					+ "	</Folder>\n"
					+ "</Document>\n");
			writer.close();
			
//			TextFileWriter.writeOver(outfile, "");
//			for (String s: result) {
//				System.out.println(s);
//				TextFileWriter.writeOn(outfile, s + "\n");
//			}
			System.out.println("I'm done, sir! ... KOBOOOOOOOLD!");
		}
		
		compute = false;
		if (compute) {
			String query = ""
					+ "SELECT * "
					+ "FROM Author "
					+ "WHERE name LIKE 'n%'"
					;
			List<Row> result = sql_answerQuery(query);
			System.out.println();  // Leerzeile für bessere Formatierung der Ausgabe
			
			Row r = result.get(0);
			StructType structure = r.schema();
			String[] fieldNames = structure.fieldNames();
			String schema = "[";
			boolean first = true;
			for (String field: fieldNames) {
				if (first) {
					schema += field;
					first = false;
				} else {
					schema += "," + field;
				}
			}
			schema += "]";
			
			System.out.println(schema);
			for (int i = 0; i < 10; i++) {
				r = result.get(i);
				System.out.println(r.toString());
			}
		}
		
		
		
		// output
//		for (String s: out) {
//			System.out.println(s);
//		}
		
	}
}