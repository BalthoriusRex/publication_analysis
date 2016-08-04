package de.bigdprak.ss2016;

public class GenerateMapForCountry {

	public static void main(String args[])
	{
		String input = ""; // country name, empty string or "Globus"
		//input = "Globus;
		//input = "Spain";
		//input = "Germany";
		
		String FoS = ""; // Field of Study
		int edges = -1;
		//FoS = "Mathematics";        edges = 2;
		//FoS = "Risk analysis";      edges = 8;
		//FoS = "Medicine";           edges = 8;
		
		if (!input.equals("Globus") && !FoS.equals("")) {
			String pathAff = "./Visualisierung/affiliations_top_1000.txt";
			String pathInputXML = "./Visualisierung/Karten/Xml/mapCoauthorship_input.xml";
			String pathCoAuthors = "./Visualisierung/edges_by_field_on_" + FoS.replace(" ", "_") + ".txt";
			int glyphSize = 3;
			int maxEdgeLevel = edges == -1 ? 8 : edges;
			boolean countryLevel = false;
			LocationDecoder.decodeLocations(input, pathAff);
			MapCoauthorships.initializeMapCoauthorships(pathAff, pathInputXML, pathCoAuthors, glyphSize, maxEdgeLevel, countryLevel);
			
		} else if (input.equals("")) {
			String pathAff = "./Visualisierung/affiliations_top_1000.txt";
			String pathInputXML = "./Visualisierung/Karten/Xml/mapCoauthorship_input.xml";
			String pathCoAuthors = "./Visualisierung/coauthorships_complete_reduced.txt";
			int glyphSize = 3;
			int maxEdgeLevel = 3;
			boolean countryLevel = false;
			LocationDecoder.decodeLocations(input, pathAff);
			MapCoauthorships.initializeMapCoauthorships(pathAff, pathInputXML, pathCoAuthors, glyphSize, maxEdgeLevel, countryLevel);
			
		} else if (input.equals("Globus")) {
			String pathAff = "./Visualisierung/countries_count.txt";
			String pathInputXML = "./Visualisierung/Karten/Xml/mapCoauthorship_input.xml";
			String pathCoAuthors = "./Visualisierung/coauthorships_by_country.txt";
			int glyphSize = 0;
			int maxEdgeLevel = 8;
			boolean countryLevel = true;
			LocationDecoder.decodeCountries(pathAff);
			MapCoauthorships.initializeMapCoauthorships(pathAff, pathInputXML, pathCoAuthors, glyphSize, maxEdgeLevel, countryLevel);
			
		} else {
			String pathAff = "./Visualisierung/affiliations_top_1000.txt";
			String pathInputXML = "./Visualisierung/Karten/Xml/mapCoauthorship_input.xml";
			String pathCoAuthors = "./Visualisierung/coauthorships_complete_reduced.txt";
			int glyphSize = 15;
			int maxEdgeLevel = 8;
			boolean countryLevel = false;
			LocationDecoder.decodeLocations(input, pathAff);
			MapCoauthorships.initializeMapCoauthorships(pathAff, pathInputXML, pathCoAuthors, glyphSize, maxEdgeLevel, countryLevel);
		}
	}
}
