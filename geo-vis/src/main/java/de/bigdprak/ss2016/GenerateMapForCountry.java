package de.bigdprak.ss2016;

public class GenerateMapForCountry {

	public static void main(String args[])
	{
		String input = "Globus"; // country name, empty string or "Globus"
		
		
		
		if (input.equals("")) {
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
		
		
//		String pathAff;
//		//pathAff = "./Visualisierung/affiliations_top_1000.txt";
//		pathAff = "./Visualisierung/countries_count.txt";
//		
//		LocationDecoder.decodeLocations(
//				input, 
//				pathAff
//		);
//		MapCoauthorships.initializeMapCoauthorships(
//				pathAff,
//				"./Visualisierung/Karten/Xml/mapCoauthorship_input.xml", 
//				"./Visualisierung/coauthorships_by_country.txt"
//		);
	}
}
