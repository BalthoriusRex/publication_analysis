package de.bigdprak.ss2016;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import de.bigdprak.ss2016.database.Country;
import de.bigdprak.ss2016.database.Location;
import de.bigdprak.ss2016.utils.UTF8Writer;

public class LocationDecoder {
	
	private static SparkConf conf;
	private static JavaSparkContext sc;
	private static SQLContext sqlContext;
	private static String folder;

	public static boolean isRemote(String[] args) {
		boolean isRemote = false;
		if (args.length > 1) {
			isRemote = args[1].equalsIgnoreCase("remote");
		}
		
		return isRemote;
	}
	
	public static void init(boolean isRemote) {
		
		System.out.println("Working on " + (isRemote ? "remote PC" : "local PC" + "!"));
		
		String user = isRemote ? "bigprak" : "balthorius"; 
		folder = "/home/"+user+"/progs/hadoop/input/";
	}
	
	public static void initSpark() {
		SimpleApp.setFileNames(folder);
		// initialize Spark - start
		conf = new SparkConf()
						.setAppName("Simple Application")
						.setMaster("local")
						.set("spark.driver.maxResultSize", "3g");
		
	    Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
	
		sc = new JavaSparkContext(conf);
		sqlContext = new org.apache.spark.sql.SQLContext(sc);
		SimpleApp.buildTables(sc, sqlContext);
		SimpleApp.buildViews(sc, sqlContext);
	}
	
	public static void closeSpark() {
		sc.close();
	}
	
	public static String printLocation(Location loc) {
		return "" + loc.getName() + "\t" + loc.getCountry() + "\t" + loc.getLongitude() + "\t" + loc.getLatitude();
	}
	
	public static void convertLocationsToXML(String affiliationFile, String locationFile, String xmlFile) {
		try {
			UTF8Writer wr = new UTF8Writer(xmlFile);
			wr.clear();
			
			BufferedReader br_aff = new BufferedReader(new FileReader(affiliationFile));
			BufferedReader br_loc = new BufferedReader(new FileReader(locationFile));
			
			br_aff.readLine();
			br_aff.readLine();
			
			String line_aff = null;
			String line_loc = null;
			
			wr.append(""
					+ "<?xml version='1.0' encoding='UTF-8'?>\n"
					+ "<kml xmlns='http://www.opengis.net/kml/2.2'>\n"
					+ "\t" + "<Document>\n"
					+ "");
			
			while ((line_loc = br_loc.readLine()) != null) {
				String[] parts_loc = line_loc.split("\t");
				String[] parts_aff = null;
				
				while (true) {
					line_aff = br_aff.readLine();
					parts_aff = line_aff.split("\t");
					if (parts_aff[2].equals(parts_loc[0])) {
						break;
					}
				}
				
				wr.append(""
						+ "\t" + "\t" + "<Placemark id='"  + parts_loc[0] +  "'>\n"
						+ "\t" + "\t" + "\t" + "<name>" + parts_loc[1] + "</name>\n"
						+ "\t" + "\t" + "\t" + "<description>" + parts_aff[0] + "</description>\n"
						+ "\t" + "\t" + "\t" + "<Point>\n"
						+ "\t" + "\t" + "\t" + "\t" + "<coordinates>" + parts_loc[2] + "," + parts_loc[3] + "</coordinates>\n"
						+ "\t" + "\t" + "\t" + "</Point>\n"
						+ "\t" + "\t" + "</Placemark>\n"
						+ "");
				
			}
			
			wr.append(""
					+ "\t" + "</Document>\n"
					+ "</kml>\n"
					+ "");
			
			
			br_aff.close();
			br_loc.close();
			wr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void generateCountries(String locations_file, String countries_file) {
		try {
			Geocoding.init_key_rotation();
			
			HashMap<String, Country> map = new HashMap<String,Country>();
			
			BufferedReader br = new BufferedReader(new FileReader(locations_file));
			
			String line;
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("\t");
				String countryName = parts[1];
				
				if (map.containsKey(countryName)) {
					continue;
				}
				
				JSONArray jsonArray = null;
				try {
					jsonArray = Geocoding.getJSONResult(countryName);
				} catch (LimitExceededException e) {
					System.out.println("Reached limit of all geocoding keys...");
					//e.printStackTrace();
					break;
				}
				
				double longitude = 0.;
				double latitude = 0.;
				String continent = null;
				
				for (int i = 0; i < jsonArray.length(); i++) {
					continent = null;
					
					JSONObject content = jsonArray.getJSONObject(i);
					JSONObject coords = null;
					if (coords == null) {
						try {
							coords = content.getJSONObject("bounds").getJSONObject("northeast");
						} catch (JSONException e) {}
					}
					if (coords == null) {
						try {
							coords = content.getJSONObject("bounds").getJSONObject("southwest");
						} catch (JSONException e) {}
					}
					if (coords == null) {
						try {
							coords = content.getJSONObject("geometry");
						} catch (JSONException e) {}
					}
					
					if (coords == null) {
						continue;
					} else {
						longitude = coords.getDouble("lng");
						latitude = coords.getDouble("lat");
					}
					
					try {
						String country_ref = content.getJSONObject("components").getString("country");
						
						if (countryName.equals(country_ref)) {
							try {
								continent = content.getJSONObject("components").getString("continent");
							} catch (JSONException e) {}
							break;
						} else {
							continue;
						}
					} catch (JSONException e) {
						System.err.println("[Country] " + countryName);
						e.printStackTrace();
					}
				}
				
				System.out.println(countryName + " -> " + continent + "\t  [" + longitude + "," + latitude + "]");
				
				Country country = new Country(countryName, continent, longitude, latitude);
				if (!map.containsKey(countryName)) {
					map.put(countryName, country);
				} else {
					Country ref = map.get(countryName);
					if (ref.getContinent() == null) {
						if (country.getContinent() != null) {
							map.put(countryName, country);
						}
					}
				}
			}
			br.close();
			
			UTF8Writer wr = new UTF8Writer(countries_file);
			wr.clear();
			for (String countryName: map.keySet()) {
				Country country = map.get(countryName);
				wr.appendLine(""
					+ country.getName()
					+ "\t"
					+ country.getContinent()
					+ "\t"
					+ country.getLongitude()
					+ "\t"
					+ country.getLatitude()
					+ "");
			}
			
			
			wr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
		
	public static void generateLocations(String affiliations_file, String outfile) {
		try {
			Geocoding.init_key_rotation();
			
			UTF8Writer wr = new UTF8Writer(outfile);
			wr.clear();
			
			BufferedReader br = new BufferedReader(new FileReader(affiliations_file));
			br.readLine();
			br.readLine();
			
			String line;
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("\t");
				long id = Long.parseLong(parts[1]);
				String normalizedName = parts[2];
				
				JSONArray jsonArray = null;
				try {
					jsonArray = Geocoding.getJSONResult(normalizedName);
				} catch (LimitExceededException e) {
					System.out.println("Reached limit of all geocoding keys...");
					//e.printStackTrace();
					break;
				}
				
				double longitude = 0.;
				double latitude = 0.;
				String country = null;
				
				for (int i = 0; i < jsonArray.length(); i++) {
					country = null;
					
					JSONObject content = jsonArray.getJSONObject(i);
					JSONObject coords = null;
					if (coords == null) {
						try {
							coords = content.getJSONObject("bounds").getJSONObject("northeast");
						} catch (JSONException e) {}
					}
					if (coords == null) {
						try {
							coords = content.getJSONObject("bounds").getJSONObject("southwest");
						} catch (JSONException e) {}
					}
					if (coords == null) {
						try {
							coords = content.getJSONObject("geometry");
						} catch (JSONException e) {}
					}
					
					if (coords == null) {
						continue;
					} else {
						longitude = coords.getDouble("lng");
						latitude = coords.getDouble("lat");
					}
					
					try {
						country = content.getJSONObject("components").getString("country");
						break;
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
				
				System.out.println(normalizedName + " -> " + country);
				
				Location loc = null;
				if (country != null) {
					loc = new Location(id, normalizedName, country, longitude, latitude);
					String loc_line = printLocation(loc);
					wr.appendLine(loc_line);
				}
			}
			
			br.close();
			wr.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("serial")
	public static void aggregateAffiliationsToCountries(String locations_file, String countries_file, String affiliations_countries_file) {
		initSpark();
		
		// create Table Location
		sqlContext.createDataFrame(
			// create JavaRDD
			sc.textFile(locations_file).map(
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
			sc.textFile(locations_file).map(
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
		
//		DataFrame content = sqlContext.sql(""
//				+ "SELECT paperID, normalizedAffiliationName "
//				+ "FROM PaperAuthorAffiliation "
//				+ "GROUP BY paperID, normalizedAffiliationName");
		
		DataFrame table_PAA = sqlContext
				.table("PaperAuthorAffiliation")
				.select("paperID", "normalizedAffiliationName")
				.dropDuplicates();
		DataFrame table_Loc = sqlContext
				.table("Location")
				.select("name", "country");
		DataFrame joined = table_PAA
							.join(table_Loc, 
									table_PAA.col("normalizedAffiliationName")
									.equalTo(
									table_Loc.col("name"))
							)
							.groupBy(
									table_PAA.col("paperID"), 
									table_Loc.col("name"))
							.org$apache$spark$sql$GroupedData$$df
							.select(table_PAA.col("paperID"), table_Loc.col("name"))
							.dropDuplicates()
							;
		List<Row> result = joined.collectAsList();
		
		UTF8Writer writer = new UTF8Writer(affiliations_countries_file);
		writer.clear();
		for (Row row: result) {
			String line = "";
			for (int i = 0; i < row.length(); i++) {
				if (i == 0) {
					line += row.get(i);
				} else {
					line += "\t" + row.get(i);
				}
			}
			writer.appendLine(line);
		}
		writer.close();
		
		
//		String query = ""
//				+ "SELECT "
//					+ "PAA.paperID as paperID,  "
//					+ "L.name as country,  "
//					+ "COUNT(paperID, country) "
//				+ "FROM "
//					+ "PaperAuthorAffiliation PAA "
//					+ "JOIN "
//					+ "Location L "
//					+ "ON PAA.normalizedAffiliationName = L.name "
//				+ "GROUP BY "
//					+ "PAA.paperID, L.name"
//				+ "";
//		List<Row> result = SimpleApp.sql_answerQuery(sqlContext, query);

//		UTF8Writer writer = new UTF8Writer(affiliations_countries_file);
//		SimpleApp.printResultsToFile(writer, query, result, "  ");
//		writer.close();
		
		
		closeSpark();
	}
	
	public static void main(String[] args) {
		boolean isRemote = isRemote(args);
		init(isRemote);
		
		String affiliations_locations_file = "./Visualisierung/affiliations_top_1000.txt";
		String affiliations_countries_file = folder + "affiliations_countries.txt";//"./Visualisierung/affiliations_countries.txt";
		String locations_file = isRemote ? folder + "locations.txt" : "./Visualisierung/locations.txt";
		String countries_file = isRemote ? folder + "countries.txt" : "./Visualisierung/countries.txt";
		String xml_locations_file = "./Visualisierung/Karten/Xml/locations.xml";
		String xml_countries_file = "./Visualisierung/Karten/Xml/countries.xml";
		
//		generateLocations(affiliations_locations_file, locations_file);
//		generateCountries(locations_file, countries_file);

//		convertLocationsToXML(affiliations_locations_file, locations_file, xml_locations_file);
		
		aggregateAffiliationsToCountries(locations_file, countries_file, affiliations_countries_file);
//		convertLocationsToXML(null, countries_file, xml_countries_file);
	}
}
