package de.bigdprak.ss2016;

// code by http://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
// [23.05.2016 17:40]

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import de.bigdprak.ss2016.utils.RandomAccessFileCoordinateWriter;


public class Geocoding {

	private final static long SLEEPTIME = 1000;
	private final String USER_AGENT = "Mozilla/5.0";

	private static int offset;
	
	public static void main(String[] args) throws Exception {
		
		if(args.length > 1)
		{
			offset = Integer.parseInt(args[1]);
		}
		else
		{
			offset = 0;
		}
		
		Geocoding geo = new Geocoding();
		
		RandomAccessFileCoordinateWriter.initializeReader("./Visualisierung/AffiliationsTest.txt", offset);
		
		

		
		JSONObject json;
		double lng;
		double lat;
		
		
		//Einlesen von Affiliations
		String[] locations = new String[10];
		for(int i = 0; i < 10; i++)
		{
			String locTemp =  RandomAccessFileCoordinateWriter.readNextAffiliation();
			locations[i] = locTemp;

			System.out.println("loc: " + locTemp);
			json = geo.getCoordsByName(locTemp);
			if(json != null)
			{
				lng = json.getDouble("lng");
				lat = json.getDouble("lat");
				
				//write
				RandomAccessFileCoordinateWriter.writeCoords(lng, lat);
				
				
				System.out.println("lng: " + lng);
				System.out.println("lat: " + lat);
				System.out.println("_______");
			}
			else
			{
				System.out.println("No data");
				System.out.println("_______");
			}
			
			
		}
		
		
		
		/*
		 String[] locations = new String[] { "effat university",
				"alnylam pharmaceuticals", "ştefan cel mare university of suceav" };

		
		for (String loc : locations) {
			JSONObject json = geo.getCoordsByName(loc);
			if(json != null)
			{
				double lat = json.getDouble("lat");
				double lng = json.getDouble("lng");
				System.out.println("Location: " + loc);
				System.out.println("     lat: " + lat);
				System.out.println("     lng: " + lng);
				System.out.println("--------------------------------");
			}
		}*/
		
		
		RandomAccessFileCoordinateWriter.closeReader();
	}

	public JSONObject getCoordsByName(String location_name)
			throws JSONException, IOException, LimitExceededException {
		
		try {
			Thread.sleep(Geocoding.SLEEPTIME);
		} catch (InterruptedException e) {
			System.err.println(""
					+ "[ERROR] Geocoder did not sleep well...\n"
					+ e.getMessage());
		}

		Geocoding http = new Geocoding();

		String format = "json";

		String user_key = "f1375e2b960b93f1538b7a4b636a7ffd";
		StringBuffer response = http.sendGet(format, location_name, user_key);

		JSONObject obj = new JSONObject(response.toString());

		JSONObject access = obj.getJSONObject("rate");

		int remaining = access.getInt("remaining");

		System.out.println(access.toString());
		
		if (remaining == 0) {
			throw new LimitExceededException();
		}

		JSONArray arr = obj.getJSONArray("results");
		String content = "";
		for (int i = 0; i < arr.length(); i++) 
		{
			content += arr.getString(i);
		}
		System.out.println(content);
		/**
		 * TODO:
		 * Fehler:
		 * Exception in thread "main" org.codehaus.jettison.json.JSONException: JSONObject["bounds"] not found.
		 *	at org.codehaus.jettison.json.JSONObject.get(JSONObject.java:360)
		 *	at org.codehaus.jettison.json.JSONObject.getJSONObject(JSONObject.java:454)
		 *	at de.bigdprak.ss2016.Geocoding.getCoordsByName(Geocoding.java:117)
		 *	at de.bigdprak.ss2016.Geocoding.main(Geocoding.java:45)
		 * Hier muss also noch überprüft werden ob in contents auch Dinge stehen mit denen wir arbeiten können (Auch wenn es nicht bounds und anderes Ding sind).
		 */
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
	private StringBuffer sendGet(String format, String location, String user_key)
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
