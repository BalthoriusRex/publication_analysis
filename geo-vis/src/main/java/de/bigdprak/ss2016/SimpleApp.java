package de.bigdprak.ss2016;

/* SimpleApp.java */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

import de.bigdprak.ss2016.database.Affiliation;
import de.bigdprak.ss2016.database.Author;
import de.bigdprak.ss2016.database.ConferenceSeries;
import de.bigdprak.ss2016.database.FieldOfStudy;
import de.bigdprak.ss2016.database.FieldOfStudyHierarchy;
import de.bigdprak.ss2016.database.Journal;
import de.bigdprak.ss2016.database.Paper;
import de.bigdprak.ss2016.database.PaperAuthorAffiliation;
import de.bigdprak.ss2016.database.PaperKeyword;
import de.bigdprak.ss2016.database.PaperReference;
import de.bigdprak.ss2016.database.PaperURL;
import de.bigdprak.ss2016.database.View_pID_affID_affName;
import de.bigdprak.ss2016.utils.UTF8Writer;

public class SimpleApp {

//	public static final String TAG_AFFILIATION_NORMALIZED = "AffiliationNameNormalized";
	public static final String TAG_AFFILIATION_FULLNAME   = "name"; 
	
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
		//file_PaperAuthorAffiliations = folder + "PaperAuthorAffiliations.txt";
		//file_PaperAuthorAffiliations = folder + "reduced_top_1000_PaperAuthorAffiliations.txt";
		file_PaperAuthorAffiliations = folder + "reduced_top_100_PaperAuthorAffiliations.txt";
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
	public static int getLineCount(JavaSparkContext sc, String input_file) {

		// input_file = "hdfs:///users/bigprak/input/Affiliations.txt";
		//master = "spark://wdi06.informatik.uni-leipzig.de:7077";
		
//		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster(master);
//		
//	    Logger.getLogger("org").setLevel(Level.ERROR);
//	    Logger.getLogger("akka").setLevel(Level.ERROR);
//	
//		JavaSparkContext sc = new JavaSparkContext(conf);
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
//		sc.close();
		
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
		
		/*
		// create Table ConferenceInstance
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_ConferenceInstances).map(
				new Function<String, ConferenceInstance>() {
					public ConferenceInstance call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new ConferenceInstance(parts);
					}
				}
			),
			ConferenceInstance.class)
		.registerTempTable("ConferenceInstance");
		*/
		
		// create Table ConferenceSeries
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_Conferences).map(
				new Function<String, ConferenceSeries>() {
					public ConferenceSeries call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new ConferenceSeries(parts);
					}
				}
			),
			ConferenceSeries.class)
		.registerTempTable("ConferenceSeries");
		
		// create Table FieldOfStudy
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_FieldsOfStudy).map(
				new Function<String, FieldOfStudy>() {
					public FieldOfStudy call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new FieldOfStudy(parts);
					}
				}
			),
			FieldOfStudy.class)
		.registerTempTable("FieldOfStudy");
		
		// create Table FieldOfStudyHierarchy
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_FieldOfStudyHierarchy).map(
				new Function<String, FieldOfStudyHierarchy>() {
					public FieldOfStudyHierarchy call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new FieldOfStudyHierarchy(parts);
					}
				}
			),
			FieldOfStudyHierarchy.class)
		.registerTempTable("FieldOfStudyHierarchy");
		
		// create Table Journal
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_Journals).map(
				new Function<String, Journal>() {
					public Journal call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new Journal(parts);
					}
				}
			),
			Journal.class)
		.registerTempTable("Journal");
		
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
		
		// create Table PaperKeyword
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_PaperKeywords).map(
				new Function<String, PaperKeyword>() {
					public PaperKeyword call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new PaperKeyword(parts);
					}
				}
			),
			PaperKeyword.class)
		.registerTempTable("PaperKeyword");
		
		// create Table PaperReference
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_PaperReferences).map(
				new Function<String, PaperReference>() {
					public PaperReference call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new PaperReference(parts);
					}
				}
			),
			PaperReference.class)
		.registerTempTable("PaperReference");
		
		// create Table PaperURL
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_PaperUrls).map(
				new Function<String, PaperURL>() {
					public PaperURL call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new PaperURL(parts);
					}
				}
			),
			PaperURL.class)
		.registerTempTable("PaperURL");
		
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
	private static void buildViews(JavaSparkContext sc, SQLContext sqlContext) {

		// create Table View_pID_affID_affName
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_PaperAuthorAffiliations).map(
				new Function<String, View_pID_affID_affName>() {
					public View_pID_affID_affName call(String line) throws Exception {
						String[] parts = line.split("\t");
						return new View_pID_affID_affName(parts);
					}
				}
			),
			View_pID_affID_affName.class)
		.registerTempTable("View_pID_affID_affName");
	}
	
	/**
	 * Instead of collecting all the results into a list, this method directly writes each result 
	 * as new line to the given output file.
	 * @param sqlContext
	 * @param query Your query.
	 * @param writer A UTF8Writer assigned to the desired output file.
	 * @return Number of results (line count)
	 */
	public static long sql_printQueryResult(SQLContext sqlContext ,String query, String output_file) {
		
		System.out.println(""
				+ "[Method] sql_printQueryResult\n"
				+ "[CURRENT JOB] Answer Query\n"
				+ "       query: " + query);
		
		final UTF8Writer writer = new UTF8Writer(output_file);
		writer.clear();
		writer.appendLine(query);
		writer.appendLine("");
		
		String queryUpper = query.toUpperCase();
		String projections = queryUpper.split("SELECT ")[1].split(" FROM ")[0];
		final int countColoumns = projections.split(",").length;
		
		DataFrame content = sqlContext.sql(query);
		@SuppressWarnings("serial")
		long result = content.javaRDD().map(
			new Function<Row, Integer>() {
				public Integer call(Row row) {
					
					// print each line to file...
					String line = "";
					for (int i = 0; i < countColoumns; i++) {
						if (i > 0) {
							line += "\t";
						}
						line += row.get(i);
					}
					writer.appendLine(line);
					
					return 1;
				}
			}
		).reduce(
			new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer a, Integer b) {
					return a + b;
				}
			}
		);
		
		writer.close();
		
		return result;
	}
	
	@SuppressWarnings("serial")
	public static List<Row> sql_answerQuery(SQLContext sqlContext ,String query) {
		
		System.out.println(""
				+ "[Method] sql_answerQuery\n"
				+ "[CURRENT JOB] Answer Query\n"
				+ "       query: " + query);
		
		long t_start = System.currentTimeMillis();
		DataFrame content = sqlContext.sql(query);
		List<Row> result = content.javaRDD().map(
			new Function<Row, Row>() {
				public Row call(Row row) {
					return row;
				}
			}
		).collect();
		long t_end = System.currentTimeMillis();
		long ms = t_end - t_start;
		long s = ms / 1000;
		long m = s / 60;
		long h = m / 60;
		ms = ms - s * 1000;
		s = s - m * 60;
		m = m - h * 60;
		System.out.println("[DURATION] " + h + "h " + m + "m " + s + "s " + ms + "ms");
		
		return result;
	}
	
	/**
	 * Prints the results of the given query to the end of the outputfile.
	 * Uses UTF8 encoding.
	 * @param outputFile
	 * @param query
	 * @param results
	 * @param projection_separator 
	 */
	public static void appendResultsToFile(UTF8Writer writer, String query, List<Row> results, String projection_separator) {
		String queryUpper = query.toUpperCase();
		String projections = queryUpper.split("SELECT ")[1].split(" FROM ")[0];
		int countColoumns = projections.split(projection_separator).length;
		
		for (Row row: results) {
			String line = "";
			//String s = row.toString();
			//String[] parts = s.substring(1, s.length()-1)
			//				  .split(",");
			for (int i = 0; i < countColoumns; i++) {
				if (i > 0) {
					line += "\t";
				}
				line += row.get(i);
			}
			writer.appendLine(line);
		}
	}
	
	/**
	 * Prints the given query in the first line each row as tab-separated line to the outputFile.
	 * Uses UTF8 encoding.
	 * @param wr
	 * @param query
	 * @param results
	 * @param projection_separator 
	 */
	public static void printResultsToFile(UTF8Writer writer, String query, List<Row> results, String projection_separator) {
		String queryUpper = query.toUpperCase();
		String projections = queryUpper.split("SELECT ")[1].split(" FROM ")[0];
		int countColoumns = projections.split(projection_separator).length;
		
		writer.clear();
		writer.appendLine(query);
		writer.appendLine("");
		if (results != null) {
			for (Row row: results) {
				String line = "";
				//String s = row.toString();
				//String[] parts = s.substring(1, s.length()-1)
				//				  .split(",");
				for (int i = 0; i < countColoumns; i++) {
					if (i > 0) {
						line += "\t";
					}
					line += row.get(i);
				}
				writer.appendLine(line);
			}
		}
	}
	
	/**
	 * Die Methode setzt die Query "SELECT DISTINCT paperID FROM PaperAuthorAffiliation" um.
	 * @param outfile
	 */
	public static void getPaperIDs(String folder, String outfile) {
		
		// Zeilenanzahl:  337000600
		// größte ID:    2252279034
		//System.err.println(Long.MAX_VALUE); // 9223372036854775807
		int lineCount =               337000600;
		long maxID = Long.parseLong("2252279034");
		
		/*
		try {
			BufferedReader br = new BufferedReader(new FileReader(SimpleApp.file_PaperAuthorAffiliations));
			String line = null;
			int lineIndex = 0;
			int percent = 0;
			int tmp_percent = 0;
			while ((line = br.readLine()) != null) {
				tmp_percent = (lineIndex * 100) / lineCount;
				if (tmp_percent > percent) {
					percent = tmp_percent;
					if (percent % 10 == 0) {
						System.err.println("progress: " + percent + "%");
					}
				}
				if (lineIndex % 1000000 == 0) {
					System.err.println("line:     " + lineIndex + " of " + lineCount + " max: " + maxID);
				}
				
				long id = Long.parseLong(line.split("\t")[0], 16);
				if (id > maxID) { maxID = id; }
				lineIndex++;
			}
			br.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
		System.out.println("maxID: " + maxID + " (" + lineCount + " lines)");
		long[] limit = new long[11];
		limit[0] = 0;
		limit[10] = maxID;
		long ten_percent = maxID / 10;
		for (int i = 1; i < 11; i++) {
			limit[i] = limit[i-1] + ten_percent;
		}
		
		
		for (int i = 0; i < 10; i++) {
			long min = limit[i];
			long max = limit[i+1];
			
			System.out.println("extracting paperIDs in range [" + min + " - " + max + "]");
			
			Set<Long> IDs = new HashSet<Long>();
			
			long t_start = System.currentTimeMillis();
			
			try {
				BufferedReader br = new BufferedReader(new FileReader(SimpleApp.file_PaperAuthorAffiliations));
				String line = null;
				while ((line = br.readLine()) != null) {
					long id = Long.parseLong(line.split("\t")[0], 16);
					if (id >= min) {
						if (id < max) {
							IDs.add(id);
						}
					}
				}
				br.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			long t_end = System.currentTimeMillis();
			double minutes = (t_end - t_start) / 60000;
			System.out.println("extracted " + IDs.size() + " paperIDs in " + minutes + " minutes...");

			String path = folder + "IDs_" + i + ".txt";
			System.out.println("printing paperIDs in range [" + min + " - " + max + "] to file " + path);
			UTF8Writer wr = new UTF8Writer(path);
			wr.clear();
			t_start = System.currentTimeMillis();
			for (long id: IDs) {
				wr.appendLine(""+id);
			}
			t_end = System.currentTimeMillis();
			minutes = (t_end - t_start) / 60000;
			System.out.println("printed " + IDs.size() + " paperIDs in " + minutes + " minutes...");
			wr.close();
		}
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
		
		
		
		// initialize Spark - start
		SparkConf conf = new SparkConf()
								.setAppName("Simple Application")
								.setMaster(master)
								.set("spark.driver.maxResultSize", "3g");
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		buildTables(sc, sqlContext);
		buildViews(sc, sqlContext);
		// initialize Spark - end
		
		
		
		//path = "hdfs:///users/bigprak/input/Affiliations.txt";
		
		String[] out = new String[files.length]; 
		
		boolean countLines = false;
		if (countLines) {
			// compute linecount for each file
			for (int i = 0; i < files.length; i++) {
				String p = files[i];
				int linecount = SimpleApp.getLineCount(sc, p);
				//int linecount = 1;
				out[i] = new String(p + ":\t" + linecount);
			}
		}
		
		boolean compute;
		
		compute = false;
		if (compute) {
			String outfile = folder + "query_results_authors_with_id_smaller_7950.txt";
			String query = ""
					+ "SELECT authorID, name "
					+ "FROM Author "
					+ "WHERE authorID < 7950";
			List<Row> results = SimpleApp.sql_answerQuery(sqlContext, query);
			
			UTF8Writer writer = new UTF8Writer(outfile);
			SimpleApp.printResultsToFile(writer, query, results, ",");
			writer.close();
//			List<String> authors = SimpleApp.sql_getAuthorNames(file_Authors);
//			int i = 0;
//			System.out.println("Output [");
//			for (String s : authors) {
//				i++;
//				System.out.println(i + "\t" + s);
//			}
//			System.out.println("]");
		}
		
		compute = false;
		if (compute) {
			sql_getPaperAuthorAffiliations(file_PaperAuthorAffiliations);
		}
		
		// Liste alle Autoren des Papers paperID mit entsprechenden Affiliations auf
		compute = false;
		if (compute) {
			long paperID = 2145616859;
			String outfile = folder + "query_results_information_on_paper_" + paperID + ".txt";
			String query = ""
					+ "SELECT "
						+ "paperID, "
						+ "authorID, "
						+ "affiliationID, "
						+ "originalAffiliationName, "
						+ "normalizedAffiliationName, "
						+ "authorSequenceNumber "
					+ "FROM "
						+ "PaperAuthorAffiliation "
					+ "WHERE "
						+ "paperID = " + paperID + " "
					+ "ORDER BY "
						+ "authorSequenceNumber ASC";
//			String query = ""
//					+ "SELECT p.originalPaperTitle, paa.authorSequenceNumber, a.name, paa.originalAffiliationName "
//					+ "FROM PaperAuthorAffiliation paa, Author a, Paper p "
//					//+ "FROM "
//					+ "WHERE "
//					+          "paa.paperID = " + paperID + " "
//					+ "AND " + "paa.authorID = a.authorID "
//					+ "AND " + "paa.paperID = p.paperID "
//					+ "ORDER BY paa.authorSequenceNumber ASC";
			List<Row> results = sql_answerQuery(sqlContext, query);
			UTF8Writer writer = new UTF8Writer(outfile);
			SimpleApp.printResultsToFile(writer, query, results, ",");
			writer.close();
			//for (Row row: results) {
			//	System.out.println(row.toString());
			//}
		}
		
		compute = false;
		if (compute) {
			// ermittle alle paperIDs aus PaperAuthorAffiliation
			String outfile = folder + "query_results_all_paperIDs.txt";
			String query = ""
					+ "SELECT DISTINCT paperID "
					+ "FROM PaperAuthorAffiliation";
			//List<Row> result = sql_answerQuery(sqlContext, query); // Ergebnis ist zu groß für Spark...
			//printResultsToFile(outfile, query, result);
			sql_printQueryResult(sqlContext, query, outfile);
		}
		
		compute = false;
		if (compute) {
			String outfile = folder + "query_results_all_paperIDs.txt";
			getPaperIDs(folder, outfile);
			
		}
		
//		compute = false;
//		if (compute) {
//			String query = ""
//					+ "SELECT affiliationID, normalizedAffiliationName "
//					+ "FROM PaperAuthorAffiliation "
//					+ "WHERE "
//					+ "";
//		}
		
		compute = false;
		if (compute) {
			// 
			String results_file = folder + "query_results_all_paperIDs.txt";
			String outfile = folder + "query_results_all_coauthorships.txt";
			BufferedReader br;
			UTF8Writer wr = new UTF8Writer(outfile);
			
			try {
				br = new BufferedReader(new FileReader(results_file));
				
				String line;
				long paperID;
				br.readLine(); // skip query line
				br.readLine(); // skip empty line
				
				String fake_query = ""
//					+ "SELECT p.originalPaperTitle, paa.authorSequenceNumber, a.name, paa.originalAffiliationName "
//					+ "FROM PaperAuthorAffiliation paa, Author a, Paper p "
//					+ "WHERE "
//					+          "paa.paperID = " + "paperID" + " "
//					+ "AND " + "paa.authorID = a.authorID "
//					+ "AND " + "paa.paperID = p.paperID "
//					+ "ORDER BY paa.authorSequenceNumber ASC"
					+ "";
				
				fake_query = ""
						+ "SELECT paperID, authorSequenceNumber, affiliationID, normalizedAffiliationName "
						+ "FROM PaperAuthorAffiliation "
						+ "WHERE paperID = " + "paperID"
						+ "";
				printResultsToFile(wr, fake_query, null, ",");
				
				while ((line = br.readLine()) != null) {
					
					paperID = Long.parseLong(line);
					
					String query = ""
//						+ "SELECT p.originalPaperTitle, paa.authorSequenceNumber, a.name, paa.originalAffiliationName "
//						+ "FROM PaperAuthorAffiliation paa, Author a, Paper p "
//						+ "WHERE "
//						+          "paa.paperID = " + paperID + " "
//						+ "AND " + "paa.authorID = a.authorID "
//						+ "AND " + "paa.paperID = p.paperID "
//						+ "ORDER BY paa.authorSequenceNumber ASC"
						+ "";
					query = ""
						+ "SELECT paperID, authorSequenceNumber, affiliationID, normalizedAffiliationName "
						+ "FROM PaperAuthorAffiliation "
						+ "WHERE paperID = " + paperID
						+ "";
					List<Row> result = sql_answerQuery(sqlContext, query);
					appendResultsToFile(wr, query, result, ",");
				}
				br.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			wr.close();
		}
		
		// Welche Affiliations gibt es?
		compute = false;
		if (compute) {
			String outfile = folder + "query_results_affiliations_from_paperauthoraffiliations.txt";
			String query = ""
					+ "SELECT "
						+ "COUNT(normalizedAffiliationName) as Anzahl, "
						+ "normalizedAffiliationName as Name, "
						+ "FIRST(originalAffiliationName) as Fullname "
					+ "FROM PaperAuthorAffiliation "
					//+ "WHERE NOT (normalizedAffiliationName = '') "
					+ "GROUP BY normalizedAffiliationName "
					+ "ORDER BY normalizedAffiliationName ASC"
					;
						
			List<Row> result = sql_answerQuery(sqlContext, query);
			UTF8Writer writer = new UTF8Writer(outfile);
			SimpleApp.printResultsToFile(writer, query, result, ",");
			writer.close();
		}
		
		compute = false;
		if (compute) {
			String query = ""
					+ "SELECT * "
					+ "FROM Author "
					+ "WHERE name LIKE 'n%'"
					;
			List<Row> result = sql_answerQuery(sqlContext, query);
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
			System.out.println("printed " + result.size() + " entries...");
		}
		
		compute = false;
		if (compute) {
			//String affiliation = "odense university";
			//String repl_affiliation = affiliation.replace(" ", "_");
			String outfile;
			//outfile = folder + "coauthorships_" + repl_affiliation + ".txt";
			
			//int limit = 200000;
			//int set = 0;
			//int offset = limit * set;
			
			outfile = folder + "coauthorships_complete_top_100.txt";
			String query = ""
					+ "SELECT "
						//+ "A.paperID AS paperID, "
						+ "A.affiliationID AS affID_A,  "
						+ "B.affiliationID AS affID_B,  "
//						+ "FIRST(A.normalizedAffiliationName) AS affName_A, "
//						+ "FIRST(B.normalizedAffiliationName) AS affName_B, "
						+ "COUNT(A.affiliationID, B.affiliationID) AS anzahl "
					+ "FROM "
						+ "View_pID_affID_affName A JOIN "
						+ "View_pID_affID_affName B "
						+ "ON A.paperID = B.paperID "
					+ "WHERE NOT(A.affiliationID = B.affiliationID) "
					+ "GROUP BY "
						+ "A.affiliationID, B.affiliationID"
//					+ "WHERE "
//						+ "A.normalizedAffiliationName = '" + affiliation + "' "
					+ "";
			
			List<Row> result = sql_answerQuery(sqlContext, query);
			
			UTF8Writer writer = new UTF8Writer(outfile);
			printResultsToFile(writer, query, result, "  ");
			writer.close();
		}
		
		compute = false;
		if (compute) {
			// extrahiere die 1000 häufigst auftretenden Affiliations
			String outfile = folder + "affiliations_top_1000_mit_leipzig.txt";
			String query = ""
					+ "SELECT "
						+ "COUNT(affiliationID) as Anzahl, "
						+ "affiliationID as affiliationID, "
						+ "FIRST(normalizedAffiliationName) as Name, "
						+ "FIRST(originalAffiliationName) as Fullname "
					+ "FROM PaperAuthorAffiliation "
					+ "WHERE NOT (normalizedAffiliationName = '') "
					+ "GROUP BY affiliationID "
					+ "ORDER BY Anzahl DESC "
					+ "LIMIT 1000"
					+ "";
			List<Row> result = sql_answerQuery(sqlContext, query);
			UTF8Writer writer = new UTF8Writer(outfile);
			SimpleApp.printResultsToFile(writer, query, result, ",");
			writer.close();
		}
		
		compute = false;
		if (compute) {
			// erzeuge reduzierte PaperAuthorAffiliations
			// indem nur noch Zeilen übernommen werden, deren Affiliations zu 
			// den 1000 häufigsten gehören.
			
			// output:
			// --------------------------
			// line:   337000000
			// kept:   46966629		~13,9%
			// dumped: 290033371	~86,1%
			
			String infile = folder + "affiliations_top_1000.txt";
			
			Set<Long> IDs = new HashSet<Long>();
			int target_affiliation_count = 100;
			try {
				BufferedReader br = new BufferedReader(new FileReader(infile));
				br.readLine(); // skip query line
				br.readLine(); // skip empty line
				String line = null;
				// get max. target_id_count many affiliations...
				int id_nr = 0;
				while ((line = br.readLine()) != null) {
					String[] parts = line.split("\t");
					String s_id = parts[1];
					String s_name = parts[2];
					if (s_name.contains("leipzig university") || id_nr < target_affiliation_count) {
						long id = Long.parseLong(s_id);
						IDs.add(id);
						id_nr++;
					}
				}
				br.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			String outfile = folder + "reduced_top_" + target_affiliation_count + "_PaperAuthorAffiliations.txt";
			
			long count_kept = 0;
			long count_dumped = 0;
			try {
				UTF8Writer writer = new UTF8Writer(outfile);
				writer.clear();
				
				BufferedReader br = new BufferedReader(new FileReader(file_PaperAuthorAffiliations));
				br.readLine(); // skip query line
				br.readLine(); // skip empty line
				String line = null;
				while ((line = br.readLine()) != null) {
					if ((count_kept + count_dumped) % 1000000 == 0) {
						System.out.println(""
								+ "--------------------------\n"
								+ "line:   " + (count_kept + count_dumped) + "\n"
								+ "kept:   " + count_kept + "\n"
								+ "dumped: " + count_dumped);
					}
					
					String s_id = line.split("\t")[2];
					long id = s_id.isEmpty() ? -1 : Long.parseLong(s_id, 16);
					
					// schreibe nur solche Zeilen zurück, deren Affiliations wichtig sind
					if (IDs.contains(id)) {
						writer.appendLine(line);
						count_kept++;
					} else {
						count_dumped++;
					}
				}
				br.close();
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		// finde Leipzig in der Datenbank
		compute = false;
		if (compute) {
			String query = ""
					+ "SELECT normalizedAffiliationName, FIRST(originalAffiliationName) "
					+ "FROM PaperAuthorAffiliation "
					+ "WHERE normalizedAffiliationName LIKE '%leipzig%' "
					+ "GROUP BY normalizedAffiliationName";
			List<Row> result = sql_answerQuery(sqlContext, query);
			for (Row row: result) {
				System.out.println(row.toString());
			}
			// [leipzig university,Department for Small Animal Medicine, Faculty of Veterinary Medicine, University of Leipzig, An den Tierkliniken 23, 04103 Leipzig, Germany]
			// [hhl leipzig graduate school of management,HHL - Leipzig Graduate School of Management]
			// [leipzig university of applied sciences,HTWK Leipzig University of Applied Sciences]
		}
		
		compute = false;
		// filtere Zeilen aus der PaperAuthorAffiliations heraus, durch die Affiliations für ein Paper doppelt erwähnt werden.
		if (compute) {
			String query = ""
					+ "SELECT "
						+ "paperID, "
						+ "FIRST(authorID), "
						+ "affiliationID, "
						+ "FIRST(originalAffiliationName), "
						+ "FIRST(normalizedAffiliationName), "
						+ "FIRST(authorSequenceNumber) "
					+ "FROM PaperAuthorAffiliation "
					+ "GROUP BY paperID, affiliationID";
			List<Row> result = sql_answerQuery(sqlContext, query);
			String paa_without_folder = SimpleApp.file_PaperAuthorAffiliations.substring(folder.length()); 
			String outfile = folder + "no_duplicate_" + paa_without_folder;
			System.out.println(outfile);
			UTF8Writer writer = new UTF8Writer(folder + "no_duplicate_PaperAuthorAffiliations.txt");
			printResultsToFile(writer, query, result, ",");
			writer.close();
		}
		

		// destroy Spark context
		sc.close();

		System.out.println("I'm done, sir! ... KOBOOOOOOOLD!");
	}
}