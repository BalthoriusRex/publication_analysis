package de.bigdprak.ss2016;

public class GenerateMapForCountry {

	public static void main(String args[])
	{
		String pathAff = "./Visualisierung/affiliations_top_1000.txt";
		
		LocationDecoder.initializeLocationDecoder(
				"Spain", 
				pathAff
		);
		MapCoauthorships.initializeMapCoauthroships(
				pathAff,
				"./Visualisierung/Karten/Xml/mapCoauthorship_input.xml", 
				"./Visualisierung/coauthorships_complete_reduced.txt"
		);
	}
}
