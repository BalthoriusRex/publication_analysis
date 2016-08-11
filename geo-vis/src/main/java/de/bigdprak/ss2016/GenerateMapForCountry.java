package de.bigdprak.ss2016;

public class GenerateMapForCountry {

	public static void main(String args[])
	{
		
		//Eingaben um festzulegen welche Daten generiert werden sollen.
		String input = ""; // country name, empty string or "Globus"
		//input = "Globus;
		//input = "Spain";
		String FoS = "";
		//FoS = "Mathematics";
		
		//Unterscheidung um welche Fälle es sich handelt und übernahme vordefinierter Einstellungen.
		//glyphSize gibt an wie weit aggregierte Glyphen zusätzlich vergrößert werden sollen. Bei großer Fläche (Betrachtung beispielsweise weltweit) sollten diese groß sein, damit sie erkannt werden. Bei regionaler Betrachtung sorgt dies aber für zusätzliche Überdeckung
		//maxEdgeLevel gibt an welche Klassen von Kanten weggelassen werden sollen. 
		if (input.equals("") && !FoS.equals("")) {
			String pathAff = "./Visualisierung/affiliations_top_1000.txt";
			String pathInputXML = "./Visualisierung/Karten/Xml/mapCoauthorship_input.xml";
			String pathCoAuthors = "./Visualisierung/edges_by_field_on_" + FoS.replace(" ", "_") + ".txt";
			int glyphSize = 3;
			int maxEdgeLevel = 3;
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
