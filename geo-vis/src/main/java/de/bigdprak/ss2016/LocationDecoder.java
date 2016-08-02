package de.bigdprak.ss2016;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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
	private static String locationsFilter = "";

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
	
	public static void convertLocationsToXML(String affiliationFile, String locationFile, String kmlFile, String xmlFile) {
		try {
			UTF8Writer wr_kml = new UTF8Writer(kmlFile);
			UTF8Writer wr_xml = new UTF8Writer(xmlFile);
			wr_kml.clear();
			wr_xml.clear();
			
			BufferedReader br_aff = new BufferedReader(new FileReader(affiliationFile));
			BufferedReader br_loc = new BufferedReader(new FileReader(locationFile));
			
			br_aff.readLine();
			br_aff.readLine();
			
			String line_aff = null;
			String line_loc = null;
			
			String append = ""
					+ "<?xml version='1.0' encoding='UTF-8'?>\n"
					+ "<kml xmlns='http://www.opengis.net/kml/2.2'>\n"
					+ "\t" + "<Document>\n"
					+ "";
			wr_kml.append(append);
			
			append  = ""
					+ "<?xml version='1.0' encoding='UTF-8'?>\n"
					+ "\t" + "<Document>\n"
					+ "";
			wr_xml.append(append);
			
			while ((line_loc = br_loc.readLine()) != null) {
				String[] parts_loc = line_loc.split("\t");
				String[] parts_aff = null;
				
				if(!parts_loc[1].equals(locationsFilter))
				{
					continue;
				}
				
				while (true) {
					line_aff = br_aff.readLine();
					parts_aff = line_aff.split("\t");
					if (parts_aff[2].equals(parts_loc[0])) {
						break;
					}
				}
				
				append = ""
						+ "\t" + "\t" + "<Placemark id='"  + parts_loc[0] +  "'>\n"
						+ "\t" + "\t" + "\t" + "<name>" + parts_loc[1] + "</name>\n"
						+ "\t" + "\t" + "\t" + "<description>" + parts_aff[0] + "</description>\n"
						+ "\t" + "\t" + "\t" + "<Point>\n"
						+ "\t" + "\t" + "\t" + "\t" + "<coordinates>" + parts_loc[2] + "," + parts_loc[3] + "</coordinates>\n"
						+ "\t" + "\t" + "\t" + "</Point>\n"
						+ "\t" + "\t" + "</Placemark>\n"
						+ "";

				wr_kml.append(append);
				wr_xml.append(append);
				
			}
			
			append = ""
					+ "\t" + "</Document>\n"
					+ "</kml>\n"
					+ "";
			wr_kml.append(append);
			
			append = ""
					+ "\t" + "</Document>\n"
					+ "";
			wr_xml.append(append);
			
			
			br_aff.close();
			br_loc.close();
			wr_kml.close();
			wr_xml.close();
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
	/*
	public static void main(String[] args) {
		init(isRemote(args));
		//initSpark();
		String affiliations_file = "./Visualisierung/affiliations_top_1000.txt";
		String locations_file = "./Visualisierung/locations.txt";
		String xml_file = "./Visualisierung/Karten/Xml/locations_"+LOCATIONS_FILTER+".xml";
		String countries_file = "./Visualisierung/countries.txt";
		
		//generateLocations(affiliations_file, locations_file);
		//generateCountries(locations_file, countries_file);
		convertLocationsToXML(affiliations_file, locations_file, xml_file);
		
		//closeSpark();
	}*/
	
	public static void initializeLocationDecoder(String filter, String pathAff)
	{
	//	init(isRemote(args));
		
		locationsFilter = filter;
		
		//initSpark();
		String affiliations_file = pathAff;
		String locations_file = "./Visualisierung/locations.txt";
		String kml_file = "./Visualisierung/Karten/Xml/locations_USE.kml";
		String xml_file = "./Visualisierung/Karten/Xml/mapCoauthorship_input.xml";
		String countries_file = "./Visualisierung/countries.txt";
		
		//generateLocations(affiliations_file, locations_file);
		//generateCountries(locations_file, countries_file);
		convertLocationsToXML(affiliations_file, locations_file, kml_file, xml_file);
		System.out.println("Finished LocationDecoding. XML and KML ready.");
		//closeSpark();
		
	}

}
