package de.bigdprak.ss2016;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import de.bigdprak.ss2016.database.Affiliation;
import de.bigdprak.ss2016.database.Author;
import de.bigdprak.ss2016.database.ConferenceSeries;
import de.bigdprak.ss2016.database.Country;
import de.bigdprak.ss2016.database.FieldOfStudy;
import de.bigdprak.ss2016.database.FieldOfStudyHierarchy;
import de.bigdprak.ss2016.database.Journal;
import de.bigdprak.ss2016.database.Location;
import de.bigdprak.ss2016.database.Paper;
import de.bigdprak.ss2016.database.PaperAuthorAffiliation;
import de.bigdprak.ss2016.database.PaperKeyword;
import de.bigdprak.ss2016.database.PaperReference;
import de.bigdprak.ss2016.database.PaperURL;
import de.bigdprak.ss2016.database.View_pID_Country;
import de.bigdprak.ss2016.database.View_pID_affID_affName;
import de.bigdprak.ss2016.utils.UTF8Writer;

public class SparkUtility {
	
	private static SparkConf conf;
	private static JavaSparkContext sc;
	private static SQLContext sqlContext;
	
	private static String master = "local";
	//private static String file_2016KDDCupSelectedAffiliations;
	//private static String file_2016KDDCupSelectedPapers;
	private static String file_Authors;
	private static String file_Affiliations;
	//private static String file_ConferenceInstances;
	private static String file_Conferences;
	private static String file_FieldOfStudyHierarchy;
	private static String file_FieldsOfStudy;
	private static String file_Journals;
	private static String file_PaperAuthorAffiliations;
	private static String file_PaperAuthorAffiliations_by_Country;
	private static String file_PaperKeywords;
	private static String file_PaperReferences;
	private static String file_Papers;
	private static String file_PaperUrls;
	private static String file_Locations;
	private static String file_Countries;
	
	private static String folder_hadoop;
	private static String folder_vis;
	private static boolean isRemote = false;
	private static boolean limit1000;
	private static boolean limit100;
	
	private static boolean initialized = false;
	
	private static boolean isRemote(String[] args) {
		isRemote = false;
		if (args.length > 1) {
			isRemote = args[1].equalsIgnoreCase("remote");
		}
		
		return isRemote;
	}
	
	public static void init(String[] args, boolean limit1000, boolean limit100) {
		boolean isRemote = isRemote(args);
		
		System.out.println("Working on " + (isRemote ? "remote PC" : "local PC" + "!"));
		
		String user = isRemote ? "bigprak" : "balthorius"; 
		folder_hadoop = "/home/"+user+"/progs/hadoop/input/";
		folder_vis = "./Visualisierung/";

		SparkUtility.limit1000 = limit1000;
		SparkUtility.limit100 = limit100;
		
		setFileNames();
		// initialize Spark - start
		conf = new SparkConf()
						.setAppName("Simple Application")
						.setMaster(master)
						.set("spark.driver.maxResultSize", "3g");
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		sc = new JavaSparkContext(conf);
		sqlContext = new org.apache.spark.sql.SQLContext(sc);
		buildTables(sc, sqlContext);
		buildViews(sc, sqlContext);
		
		initialized = true;
	}
	
	public static void close() {
		initialized = false;
		sc.close();
	}
	
	private static void setFileNames() {

		file_Authors = folder_hadoop + "Authors.txt";
		file_Affiliations = folder_hadoop + "Affiliations.txt";
		//file_ConferenceInstances = folder_hadoop + "ConferenceInstances.txt";
		file_Conferences = folder_hadoop + "Conferences.txt";
		file_FieldOfStudyHierarchy = folder_hadoop + "FieldOfStudyHierarchy.txt";
		file_FieldsOfStudy = folder_hadoop + "FieldsOfStudy.txt";
		file_Journals = folder_hadoop + "Journals.txt";
		file_PaperAuthorAffiliations = folder_hadoop + "PaperAuthorAffiliations.txt";
		if (limit1000) {
			file_PaperAuthorAffiliations = folder_hadoop + "reduced_top_1000_PaperAuthorAffiliations.txt";
		}
		if (limit100) {
			file_PaperAuthorAffiliations = folder_hadoop + "reduced_top_100_PaperAuthorAffiliations.txt";
		}
		file_PaperAuthorAffiliations_by_Country = folder_hadoop + "reduced_countries_PaperAuthorAffiliations.txt";
		file_PaperKeywords = folder_hadoop + "PaperKeywords.txt";
		file_PaperReferences = folder_hadoop + "PaperReferences.txt";
		file_Papers = folder_hadoop + "Papers.txt";
		file_PaperUrls = folder_hadoop + "PaperUrls.txt";

		file_Locations = (isRemote ? folder_hadoop : folder_vis) + "locations.txt";
		file_Countries = (isRemote ? folder_hadoop : folder_vis) + "countries.txt";
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
		
		// create Table Location
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_Locations).map(
				new Function<String, Location>() {
					public Location call(String line) throws Exception {
						String[] parts = line.split("\t");
						
						String name = parts[0];
						String country = parts[1];
						double longitude = Double.parseDouble(parts[2]);
						double latitude = Double.parseDouble(parts[3]);
						
						return new Location(name, country, longitude, latitude);
					}
				}
			),
			Location.class)
		.registerTempTable("Location");
		
		// create Table Country
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_Countries).map(
				new Function<String, Country>() {
					public Country call(String line) throws Exception {
						String[] parts = line.split("\t");
						
						String name = parts[0];
						String continent = parts[1].equals("null") ? null : parts[1];
						double longitude = Double.parseDouble(parts[2]);
						double latitude = Double.parseDouble(parts[3]);
						
						return new Country(name, continent, longitude, latitude);
					}
				}
			),
			Country.class)
		.registerTempTable("Country");
				
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
		
		// create Table View_pID_Country
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(file_PaperAuthorAffiliations_by_Country).map(
				new Function<String, View_pID_Country>() {
					public View_pID_Country call(String line) throws Exception {
						String[] parts = line
											.substring(1, line.length() - 1)
											.split(",");
						return new View_pID_Country(Long.parseLong(parts[0]), parts[1]);
					}
				}
			),
			View_pID_Country.class)
		.registerTempTable("View_pID_Country");
	}
	
	@SuppressWarnings("serial")
	public static List<Row> sql_answerQuery(String query) {
		
		if (!initialized) {
			System.err.println("Spark is not initialized yet!\n");
			return null;
		}
		
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
	
	public static JavaSparkContext getSC() {
		if (initialized) {
			return sc;
		} else {
			System.err.println("Spark is not initialized yet!\n");
			return null;
		}
	}
	
	public static SQLContext getSQL() {
		if (initialized) {
			return sqlContext;
		} else {
			System.err.println("Spark is not initialized yet!\n");
			return null;
		}
	}
	
	public static String getFolderHadoop() {
		if (initialized) {
			return folder_hadoop;
		} else {
			System.err.println("Spark is not initialized yet!\n");
			return null;
		}
	}
	
	public static void printResults(String path_to_file, Row[] results) {
		
		UTF8Writer writer = new UTF8Writer(path_to_file);
		writer.clear();
		
		if (results != null) {
			int countColumns = results[0].length();
			
			for (Row row: results) {
				String line = "";
				for (int i = 0; i < countColumns; i++) {
					if (i > 0) {
						line += "\t";
					}
					line += row.get(i);
				}
				writer.appendLine(line);
			}
		}
		writer.close();
	}

}
