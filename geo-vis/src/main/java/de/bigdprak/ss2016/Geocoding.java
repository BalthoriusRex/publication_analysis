package de.bigdprak.ss2016;

// code by http://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
// [23.05.2016 17:40]

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import de.bigdprak.ss2016.utils.RandomAccessFileCoordinateWriter;
import de.bigdprak.ss2016.utils.TextFileReader;
import de.bigdprak.ss2016.utils.TextFileWriter;
import de.bigdprak.ss2016.utils.UTF8Writer;


public class Geocoding {
	
	private static int remaining = 2500;

	private final static long SLEEPTIME = 1000;
	private final static String USER_AGENT = "Mozilla/5.0";

	private static int offset;
	private static int limit_per_run = 20;
	private static int init_remains = -1;
	private static int limit = 1;
	
	public static void main(String[] args) {
		
		String path_results = 	"./Visualisierung/query_results_affiliations_from_paperauthoraffiliations.txt";
		String path_xml = 		"./Visualisierung/query_results_affiliations_from_paperauthoraffiliations.xml"; 
		String path_offset = 	"./Visualisierung/tmp.offset.txt";
		
		boolean convertResults = false;
		if (convertResults) {
			convertResultsFileToXML(path_results, path_xml);
		}
		
		boolean fillCoords = true;
		if (fillCoords) {
			
			if(args.length > 1) {
				Geocoding.offset = Integer.parseInt(args[1]);
			} else {
				Geocoding.offset = TextFileReader.parseOffset(path_offset);
			}
			System.err.println("Starting with offset " + Geocoding.offset);
			System.err.println("Limit per run: " + Geocoding.limit_per_run);
			
			//RandomAccessFileCoordinateWriter.initializeReader("./Visualisierung/output_query_affiliations.txt", Geocoding.offset);
			RandomAccessFileCoordinateWriter.initializeReader(path_xml, Geocoding.offset);
			
			JSONObject json;
			double lng;
			double lat;
			//Einlesen von Affiliations
			//String[] locations = new String[10];
			
			try {
				// while not EOF
				String locTemp = null;
				//for(int i = 0; i < numberOfEntries; i++)
				//int i = -1;
				while(true) // LimitExceedException breaks while-true-loop
				{
					//i++;
					locTemp = null;
					locTemp = RandomAccessFileCoordinateWriter.readNextNormalizedAffiliation();
					if(locTemp == null)
					{
						System.out.println("File ended");
						break;
					}
					//locations[i] = locTemp;
		
					System.out.println("loc: " + locTemp);
					json = Geocoding.getCoordsByName(locTemp);
					
					// falls wir über den normalizedAffiliationName kein Ergebnis bekommen
					// versuchen wir das ganze nochmal mit dem originalAffiliationName
					if (json == null) {
	
						locTemp = RandomAccessFileCoordinateWriter.readNextOriginalAffiliation();
						if(locTemp == null)
						{
							//throw new LimitExceededException(LimitExceededException.ERROR_EOF);
							System.out.println("File ended");
							break;
						}
						//locations[i] = locTemp;
		
						System.out.println("loc: " + locTemp);
						json = Geocoding.getCoordsByName(locTemp);
					}
					
					if(json != null)
					{
						lng = json.getDouble("lng");
						lat = json.getDouble("lat");
						
						//write
						RandomAccessFileCoordinateWriter.writeCoords(lng, lat);
						
						System.out.println("lng: " + lng);
						System.out.println("lat: " + lat);
						System.out.println("_______");
					} else {
						System.out.println("No data");
						System.out.println("_______");
					}
					Geocoding.offset = Geocoding.offset+1;
					
				}
			} catch (LimitExceededException e) {
				System.out.println(e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}
			// write the new offset to file to have access for tomorrows computations
			TextFileWriter.writeOver(path_offset, Geocoding.offset+"\n");
			
			System.err.println("new offset: " + Geocoding.offset);
			int read_offset = TextFileReader.parseOffset(path_offset);
			System.err.println("offset in file: " + read_offset);
			
			
	//		String[] locations = new String[] { "effat university",
	//				"alnylam pharmaceuticals", "ştefan cel mare university of suceav" };
	//
	//		for (String loc : locations) {
	//			JSONObject json = geo.getCoordsByName(loc);
	//			if(json != null)
	//			{
	//				double lat = json.getDouble("lat");
	//				double lng = json.getDouble("lng");
	//				System.out.println("Location: " + loc);
	//				System.out.println("     lat: " + lat);
	//				System.out.println("     lng: " + lng);
	//				System.out.println("--------------------------------");
	//			}
	//		}
			
			RandomAccessFileCoordinateWriter.closeReader();
		}
	}
	
	/**
	 * This method reads all lines from a materialized query output.
	 * (materialized via SimpleApp.printResultsToFile)
	 * In each coordinates line there is a whitespace of 35 characters at the end which will
	 * be overwritten later on by another service
	 * This method should only be used for file query_results_affiliations_from_paperauthoraffiliations.txt
	 * @param results_input_path
	 * @param results_XML_path
	 * @throws IOException 
	 */
	public static void convertResultsFileToXML(String results_input_path, String results_XML_path) {
		String whitespace = "";
		int whitespace_size = 35;
		for (int i = 0; i < whitespace_size; i++) {
			whitespace += " ";
		}
		
		BufferedReader reader;
		try {
			reader= new BufferedReader(new FileReader(results_input_path));
		} catch (FileNotFoundException e) {
			System.err.println(""
					+ "Could not open " + results_input_path + "\n"
					+ "Error: " + e.getMessage());
			return;
		}
		
		UTF8Writer writer = new UTF8Writer(results_XML_path);
		writer.clear();
		writer.append(""
				+ "<?xml version='1.0' encoding='UTF-8'?>\n"
				+ "<kml xmlns='http://www.opengis.net/kml/2.2'>\n"
				+ "<Document>\n"
				+ "\t" + "<Folder>\n");
		
		try {
			String line, anzahl, normalizedName, fullName;
			String[] parts;
			line = reader.readLine(); // skipping query line
			line = reader.readLine(); // skipping empty line between query and results
			
			while (true) {
				
				line = reader.readLine();
				try {
					parts = line.split("\t");
					anzahl = parts[0];
					try {
					normalizedName = parts[1];
					fullName = parts[2];
					} catch (ArrayIndexOutOfBoundsException e) {
						normalizedName = "empty";
						fullName = "empty";
					}
				} catch (NullPointerException e) {
					System.out.println("reached EOF");
					break;
				}
				String newS = ""
						+ "\t" + "\t" + "<Placemark id='"+normalizedName+"'>\n"
						+ "\t" + "\t" + "\t" + "<name>" + fullName + "</name>\n"
						+ "\t" + "\t" + "\t" + "<description>" + anzahl + "</description>\n"
						+ "\t" + "\t" + "\t" + "<Point>\n"
						+ "\t" + "\t" + "\t" + "\t" + "<coordinates></coordinates>" + whitespace + "\n"
						+ "\t" + "\t" + "\t" + "</Point>\n"
						+ "\t" + "\t" + "</Placemark>\n";
				writer.append(newS);
			}
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		writer.append(""
				+ "\t" + "</Folder>\n"
				+ "</Document>\n"
				+ "</kml>\n");
		writer.close();
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static JSONObject getCoordsByName(String location_name)
			throws JSONException, IOException, LimitExceededException {
		
		try {
			Thread.sleep(Geocoding.SLEEPTIME);
		} catch (InterruptedException e) {
			System.err.println(""
					+ "[ERROR] Geocoder did not sleep well...\n"
					+ e.getMessage());
		}

		String format = "json";

		String user_key = "f1375e2b960b93f1538b7a4b636a7ffd";
		StringBuffer response = Geocoding.sendGet(format, location_name, user_key);

		JSONObject obj = new JSONObject(response.toString());

		JSONObject access = obj.getJSONObject("rate");

		int remaining = access.getInt("remaining");
		if (init_remains < 0) {
			init_remains = remaining;
			limit = init_remains - limit_per_run;
		}

		System.out.println(access.toString());
		Geocoding.remaining = remaining - limit;// - 2460;
		System.out.println("remaining: " + Geocoding.remaining);
		if (Geocoding.remaining < 1) {
			throw new LimitExceededException(LimitExceededException.ERROR_LIMIT);
		}

		JSONArray arr = obj.getJSONArray("results");
		String content = "";
		for (int i = 0; i < arr.length(); i++) 
		{
			content += arr.getString(i);
		}
		System.out.println(content);

		if(content.length() == 0)
		{
			//No information about this affiliation!
			//Dropping information (maybe we could use an alternative service?
			return null;
		}
		
		
		obj = new JSONObject(content);
		

		JSONObject ret = null;
		
		boolean foundCoords = false;
		if (!foundCoords) {
			try {
				ret = obj.getJSONObject("bounds")
						 .getJSONObject("northeast");
				foundCoords = true;
			} catch (JSONException e) {
				System.err.println(""
						+ "[ERROR] " + e.getMessage() + "\n"
						+ "        trying next tag...");
			}
		}
		if (!foundCoords) {
			try {
				ret = obj.getJSONObject("geometry");
				foundCoords = true;
			} catch (JSONException e) {
				System.err.println(""
						+ "[ERROR] " + e.getMessage() + "\n"
						+ "        trying next tag...");
			}
		}
		if (!foundCoords) {
			System.out.println("[ERROR] did not find any coords...");
		}
		
		//obj = obj.getJSONObject("bounds");
		//obj = obj.getJSONObject("northeast");
		
		// double lat = obj.getDouble("lat");
		// double lng = obj.getDouble("lng");

		// System.out.println(obj.toString());
		// System.out.println("lat: " + lat);
		// System.out.println("lng: " + lng);
		// geocoding("04155 Leipzig, Blumenstraße 43");
		// geocoding("Universität Leipzig");

		return ret;
	}

	// HTTP GET request
	private static StringBuffer sendGet(String format, String location, String user_key)
			throws IOException {

		String location_query = location;

		location_query = URLEncoder.encode(location_query, "UTF-8");

		String url = "https://api.opencagedata.com/geocode/v1/" + format
				+ "?q=" + location_query + "&key=" + user_key;

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		
		System.out.println("URL: " + url.toString());

		// optional default is GET
		con.setRequestMethod("GET");

		// add request header
		con.setRequestProperty("User-Agent", USER_AGENT);

		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response;

	}
	


}
