package de.bigdprak.ss2016;

// code by http://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
// [23.05.2016 17:40]

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Geocoding {

	private final String USER_AGENT = "Mozilla/5.0";

	public static void main(String[] args) throws Exception {
		Geocoding geo = new Geocoding();
		String[] locations = new String[] { "effat university",
				"alnylam pharmaceuticals" };

		for (String loc : locations) {
			JSONObject json = geo.getCoordsByName(loc);
			double lat = json.getDouble("lat");
			double lng = json.getDouble("lng");
			System.out.println("--------------------------------");
			System.out.println("Location: " + loc);
			System.out.println("     lat: " + lat);
			System.out.println("     lng: " + lng);
		}
	}

	public JSONObject getCoordsByName(String location_name)
			throws JSONException, IOException, LimitExceededException {

		Geocoding http = new Geocoding();

		// System.out.println("Testing 1 - Send Http GET request");
		String format = "json";
		// String location = "effat university";
		String user_key = "f1375e2b960b93f1538b7a4b636a7ffd";
		StringBuffer response = http.sendGet(format, location_name, user_key);

		JSONObject obj = new JSONObject(response.toString());

		JSONObject access = obj.getJSONObject("rate");
		// int limit = access.getInt("limit");
		int remaining = access.getInt("remaining");
		// int reset = access.getInt("reset");
		if (remaining == 0) {
			throw new LimitExceededException();
		}

		JSONArray arr = obj.getJSONArray("results");
		String content = "";
		for (int i = 0; i < arr.length(); i++) {
			content += arr.getString(i);
		}
		obj = new JSONObject(content);
		obj = obj.getJSONObject("bounds");
		obj = obj.getJSONObject("northeast");
		// double lat = obj.getDouble("lat");
		// double lng = obj.getDouble("lng");

		// System.out.println(obj.toString());
		// System.out.println("lat: " + lat);
		// System.out.println("lng: " + lng);
		// geocoding("04155 Leipzig, Blumenstraße 43");
		// geocoding("Universität Leipzig");

		return obj;
	}

	// public static void geocoding(String addr) throws Exception {
	// // build a URL
	// String s = "http://maps.google.com/maps/api/geocode/json?"
	// + "sensor=false&address=";
	// s += URLEncoder.encode(addr, "UTF-8");
	// URL url = new URL(s);
	//
	// // read from the URL
	// Scanner scan = new Scanner(url.openStream());
	// String str = new String();
	// while (scan.hasNext())
	// str += scan.nextLine();
	// scan.close();
	//
	// // build a JSON object
	// JSONObject obj = new JSONObject(str);
	// if (!obj.getString("status").equals("OK"))
	// return;
	//
	// // get the first result
	// JSONObject res = obj.getJSONArray("results").getJSONObject(0);
	// System.out.println(res.getString("formatted_address"));
	// JSONObject loc = res.getJSONObject("geometry")
	// .getJSONObject("location");
	// System.out.println("lat: " + loc.getDouble("lat") + ", lng: "
	// + loc.getDouble("lng"));
	// }

	// HTTP GET request
	private StringBuffer sendGet(String format, String location, String user_key)
			throws IOException {

		String location_query = location;
		location_query = location_query.replace(" ", "+");
		location_query = location_query.replace(",", "%2C");
		// System.out.println(location_query);

		String url = "https://api.opencagedata.com/geocode/v1/" + format
				+ "?q=" + location_query + "&key=" + user_key;
		// System.out.println(url);

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		// add request header
		con.setRequestProperty("User-Agent", USER_AGENT);

		// int responseCode = con.getResponseCode();
		// System.out.println("\nSending 'GET' request to URL : " + url);
		// System.out.println("Response Code : " + responseCode);

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
