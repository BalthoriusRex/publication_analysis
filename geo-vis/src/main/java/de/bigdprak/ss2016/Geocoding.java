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
import java.util.HashMap;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import de.bigdprak.ss2016.utils.RandomAccessFileCoordinateWriter;
import de.bigdprak.ss2016.utils.TextFileReader;
import de.bigdprak.ss2016.utils.TextFileWriter;
import de.bigdprak.ss2016.utils.UTF8Writer;

/**
 * Klasse Bestimmen von Geo-Informationen zu Affiliations.
 */
public class Geocoding {
	
	private static int whitespace_size = 35;

	private final static String USER_AGENT = "Mozilla/5.0";
	
	//Mehrere Keys um schnellere Abarbeitung zu gewährleisten
	private final static String[] available_users = { 	 "f1375e2b960b93f1538b7a4b636a7ffd"
														,"1b9169ea5b526d47d0d508680a4e6455"
														,"a604828e9f6eb55b7313d762e5562c69"
														,"bb0e107df1aa4234eea580e508f4ce86"
													};

	private static int currentUser_index = 0;
	private static int offset;
	private static long sleep_time;
	private static HashMap<String, Integer> active_keys;
	private static int count_all_keys = available_users.length;
	private static int count_active_keys;
	private static int count_inactive_keys = 0;
	
	private static boolean key_rotation_initialized = false;
	
	/**
	 * Initialisiere die Rotation der verwendeten Geocoding-Keys zur Beschleunigung
	 * des Geocodings durch abwechselnde Verwendung mehrerer Keys.
	 */
	public static void init_key_rotation() {
		// Da jede Sekunde nur eine Anfrage gestellt werden darf:
		// Wechsel nach jeder Anfrage den genutzten Key
		// -> Mehr als eine Anfrage pro Sekunde möglich
		if (!key_rotation_initialized) {
			
			currentUser_index = (int) Math.floor(Math.random() * count_all_keys);
			
			active_keys = new HashMap<String, Integer>();
			for (String key: available_users) {
				active_keys.put(key, 2500);
			}
			count_active_keys = count_all_keys - count_inactive_keys;
			sleep_time = (1000 / count_active_keys) + 1;
			
			key_rotation_initialized = true;
		}
	}
	
	/**
	 * Hauptprogramm des Geocoding.
	 * @param args
	 */
	public static void main(String[] args) {
		
		String path_results = "./Visualisierung/affiliations_top_1000.txt";
		String path_xml     = "./Visualisierung/affiliations_top_1000.xml"; 
		String path_offset  = "./Visualisierung/tmp.offset.txt";
		
		init_key_rotation();
		
		// Übersetzung der Top-Affiliations in XML-Format
		boolean convertResults = false;
		if (convertResults) {
			convertResultsFileToXML(path_results, path_xml);
			// setzt Offset-Zähler für abgehandelte Affiliations zurück
			UTF8Writer writer = new UTF8Writer(path_offset);
			writer.clear();
			writer.close();
		}
		
		// Anreichern des XML mit Geo-Koordinaten
		boolean fillCoords = true;
		if (fillCoords) {
			
			// Setzen der Offset-Variable
			if(args.length > 1) { Geocoding.offset = Integer.parseInt(args[1]);               }
			else                { Geocoding.offset = TextFileReader.parseOffset(path_offset); }
			System.err.println("Starting with offset " + Geocoding.offset);
			
			// initialisiert Reader für XML-File
			RandomAccessFileCoordinateWriter.initializeReader(path_xml, Geocoding.offset);
			
			// wiederverwendete Variablen
			JSONObject json;
			double lng;
			double lat;
			
			// Einlesen von Affiliations
			currentUser_index = 0;
			try {
			
				// while not EOF
				String locTemp = null; // loc - Location
				while(true) // LimitExceedException breaks while-true-loop
				{
					try {
						locTemp = null;
						
						// Versuch #1: ermittle Koordinaten anhand des NormalizedAffiliationName
						locTemp = RandomAccessFileCoordinateWriter.readNextNormalizedAffiliation();
						if(locTemp == null)
						{
							System.out.println("File ended");
							break;
						}
						System.out.println("loc: " + locTemp);
						json = Geocoding.getCoordsByName(locTemp);
						
						// falls wir über den normalizedAffiliationName kein Ergebnis bekommen
						// versuchen wir das ganze nochmal mit dem originalAffiliationName
						if (json == null) {
							
							// Versuch #2: ermittle Koordinaten anhand des OriginalAffiliationName
							locTemp = RandomAccessFileCoordinateWriter.readNextOriginalAffiliation();
							if(locTemp == null)
							{
								System.out.println("File ended");
								break;
							}
							System.out.println("loc: " + locTemp);
							json = Geocoding.getCoordsByName(locTemp);
						}
						
						if(json != null)
						{
							// lies Koordinaten aus, sofern vorhanden
							lng = json.getDouble("lng");
							lat = json.getDouble("lat");
							//write
							System.out.println("[lng lat]: [" + lng + " " + lat + "]");
							System.out.println("_______");
						} else {
							// sonst schreibe Default-Koordinaten zurück
							lng = 0.0;
							lat = 0.0;
							System.out.println("No data");
							System.out.println("_______");
						}
						RandomAccessFileCoordinateWriter.writeCoords(lng, lat);
						Geocoding.offset = Geocoding.offset+1;
						
					} catch (IOException e) {
						System.err.println(e.getMessage());
					}
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				System.err.println(""
						+ "Anscheinend haben alle User ihr Kontingent bei OpenCageData aufgebraucht...\n"
						+ "Versuche es morgen nochmal.\n");
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (LimitExceededException e) {
				System.err.println(""
						+ "Limit exceeded for all keys...\n"
						+ "Try again tomorrow...\n");
			}
			
			// write the new offset to file to have access for tomorrows computations
			TextFileWriter.writeOver(path_offset, Geocoding.offset+"\n");
			
			System.err.println("new offset: " + Geocoding.offset);
			int read_offset = TextFileReader.parseOffset(path_offset);
			System.err.println("offset in file: " + read_offset);
				
			RandomAccessFileCoordinateWriter.closeReader();
		}
	}
	
	/**
	 * OBSOLETE
	 * Transforms the standard results file to a file with an additional column and whitespace in this column.
	 * In the process of generating coordinates this whitespace shall be overwritten by coordinate values.
	 * @param results_input_path
	 * @param results_output_path
	 */
	@SuppressWarnings("unused")
	private static void convertResultsFileToResultsFileWithWhitespace(String results_input_path, String results_output_path) {
		String whitespace = "";
		for (int i = 0; i < whitespace_size; i++) {
			whitespace += " ";
		}
		
		UTF8Writer wr = new UTF8Writer(results_output_path);
		wr.clear();
		
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(results_input_path));
			br.readLine(); // skip query line
			br.readLine(); // skip empty line
			
			String line;
			while ((line = br.readLine()) != null) {
				wr.appendLine(line + "\t" + whitespace);
			}
			
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		wr.close();
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
	private static void convertResultsFileToXML(String results_input_path, String results_XML_path) {
		String whitespace = "";
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
				+ "\t" + "<Document>\n");
		
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
					normalizedName = parts[2];
					fullName = parts[3];
					} catch (ArrayIndexOutOfBoundsException e) {
						normalizedName = "null island";
						fullName = "Null Island";
					}
					normalizedName = normalizedName.replace("&", "and");
					fullName       =       fullName.replace("&", "and");

					normalizedName = normalizedName.replace("@", "AT");
					fullName       =       fullName.replace("@", "AT");

					normalizedName = normalizedName.replace("<", "");
					fullName       =       fullName.replace("<", "");
					
					normalizedName = normalizedName.replace(">", "");
					fullName       =       fullName.replace(">", "");
					
					normalizedName = normalizedName.replace("§", "");
					fullName       =       fullName.replace("§", "");
					
				} catch (NullPointerException e) {
					System.out.println("reached EOF");
					break;
				}
				String newS = ""

						+ "\t" + "\t" + "<Placemark id='"+fullName+"'>\n"
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
				+ "\t" + "</Document>\n"
				+ "</kml>\n");
		writer.close();
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Stellt Geocoding-Anfrage an OpenCageData Geocoding-Dienst.
	 * Über die Schlüssel lng, lat können aus dem Ergebnis Koordinaten entnommen werden.
	 * @param location_name
	 * @return
	 * 		JSONbject, aus dem im Anschluss verschiedene Informationen extrahiert werden können;
	 * 		null, falls keine Geo-Informationen vorhanden.
	 * @throws JSONException
	 * @throws IOException
	 * @throws LimitExceededException
	 */
	private static JSONObject getCoordsByName(String location_name)
			throws JSONException, IOException, LimitExceededException {
		try {
			Thread.sleep(Geocoding.sleep_time);
		} catch (InterruptedException e) {
			System.err.println(""
					+ "[ERROR] Geocoder did not sleep well...\n"
					+ e.getMessage());
		}
		
		// Ermittle nächsten Key, der für die Anfrage verwendet werden soll
		String user_key = null;
		for (int i = 0; i < count_all_keys; i++) {
			String key = available_users[currentUser_index];
			int rem = active_keys.get(key);
			currentUser_index = (currentUser_index + 1) % count_all_keys;
			if (rem > 0) {
				user_key = key;
				break;
			}
		}
		if (user_key == null) {
			throw new LimitExceededException(LimitExceededException.ERROR_LIMIT);
		}
		
		
		// Senden der Anfrage an OpenCageData
		String format = "json";
		StringBuffer response = Geocoding.sendGet(format, location_name, user_key);
		

		JSONObject obj = new JSONObject(response.toString());

		// Aktualisieren der Key-Informationen
		// schreibt jeweils neue Anzahl an verbleibenden Anfragen in Map
		JSONObject access = obj.getJSONObject("rate");
		int remaining = access.getInt("remaining");
		if (remaining == 0) {
			// sobald ein Key ausfällt, muss die Anfragezeit angepasst werden, sodass die
			// komplette Rotation weiterhin eine Dauer von 1s umfasst.
			count_inactive_keys++;
			Geocoding.sleep_time = (1000 / (count_all_keys - count_inactive_keys)) + 1;
		}
		active_keys.put(user_key, remaining);
		//System.out.println(access.toString());
		//System.out.println("key: " + user_key + " remaining: " + remaining);

		JSONArray arr = obj.getJSONArray("results");
		String content = "";
		for (int i = 0; i < arr.length(); i++) 
		{
			content += arr.getString(i);
		}
		//System.out.println(content);

		if(content.length() == 0)
		{
			//No information about this affiliation!
			//Dropping information (maybe we could use an alternative service?)
			return null;
		}
		
		
		obj = new JSONObject(content);
		
		// extrahiere Geo-Informationen aus Anfrageresultat
		// mögliche tags: geometry, bounds->northeast 
		JSONObject ret = null;
		boolean foundCoords = false;
		if (!foundCoords) {
			try {
				ret = obj.getJSONObject("geometry");
				foundCoords = true;
			} catch (JSONException e) {
				System.out.println(""
						+ "[ERROR] " + e.getMessage() + "\n"
						+ "        trying next tag...");
			}
		}
		if (!foundCoords) {
			try {
				ret = obj.getJSONObject("bounds")
						 .getJSONObject("northeast");
				foundCoords = true;
			} catch (JSONException e) {
				System.out.println(""
						+ "[ERROR] " + e.getMessage() + "\n"
						+ "        trying next tag...");
			}
		}
		if (!foundCoords) {
			System.out.println("[ERROR] did not find any coords...");
		}
	
		return ret;
	}
	
	/**
	 * Stellt Geocoding-Anfrage OpenCageData und liefert das komplette Ergebnis zurück.
	 * @param location_name
	 * @return
	 * @throws JSONException
	 * @throws IOException
	 * @throws LimitExceededException
	 */
	public static JSONArray getJSONResult(String location_name)
			throws JSONException, IOException, LimitExceededException {
		try {
			Thread.sleep(Geocoding.sleep_time);
		} catch (InterruptedException e) {
			System.err.println(""
					+ "[ERROR] Geocoder did not sleep well...\n"
					+ e.getMessage());
		}
		
		// Key-Rotation
		String user_key = null;
		for (int i = 0; i < count_all_keys; i++) {
			String key = available_users[currentUser_index];
			int rem = active_keys.get(key);
			currentUser_index = (currentUser_index + 1) % count_all_keys;
			if (rem > 0) {
				user_key = key;
				break;
			}
		}
		if (user_key == null) {
			throw new LimitExceededException(LimitExceededException.ERROR_LIMIT);
		}
		
		// tatsächliche Anfrage an OpenCageData
		String format = "json";
		StringBuffer response = Geocoding.sendGet(format, location_name, user_key);
		JSONObject obj = new JSONObject(response.toString());
		
		// Aktualisieren der Infos zur Key-Rotation
		JSONObject access = obj.getJSONObject("rate");
		int remaining = access.getInt("remaining");
		if (remaining == 0) {
			count_inactive_keys++;
			Geocoding.sleep_time = (1000 / (count_all_keys - count_inactive_keys)) + 1;
		}
		active_keys.put(user_key, remaining);

		//System.out.println(access.toString());
		//System.out.println("key: " + user_key + " remaining: " + remaining);

		JSONArray result = obj.getJSONArray("results");
		return result;
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
		
		//System.out.println("URL: " + url.toString());

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
