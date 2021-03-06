package de.bigdprak.ss2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class MapCoauthorships {

	public static final int NUMBER_OF_WRITER = 9;
	


	public static FileWriter[] writers = new FileWriter[NUMBER_OF_WRITER];
	
	public static int numberOfEdges = 0;
	public static long countMax = 0;
	
	public static int  skipped = 0;
	
	public static int duplicate = 0;
	
	//Erstellen der Kanten
	/**
	 * Erstellt Kantenlisten mit Koordinaten-Angaben für 8 Klassen von Kanten.
	 * Diese werden direkt für die Ausgabe mittels Karte benötigt, da sie die Verbindungslinien
	 * für Ko-Autorschaften erzeugen.
	 * @param authorships
	 * @param map
	 * @param glyphSize
	 * @param maxEdgeLevel
	 * @throws IOException
	 * @throws XPathExpressionException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 */
	private static void getCoauthorships(String authorships, HashMap<String, String> map, int glyphSize, int maxEdgeLevel) throws IOException, XPathExpressionException, SAXException, ParserConfigurationException
	{
		BufferedReader reader = new BufferedReader(new FileReader(new File(authorships)));
		
		String line;
		reader.readLine();	//Initialzeilen mit Querybeschreibung sollen übersprungen werden
		reader.readLine();
		
		writeOnAll("var coauthorships");
		for(int i = 0; i < NUMBER_OF_WRITER; i++)
		{
			writers[i].append(""+i);
		}
		writeOnAll(" = [");
		
		boolean initial[] = new boolean[NUMBER_OF_WRITER];
		for(int i = 0; i < NUMBER_OF_WRITER; i++)
		{
			initial[i] = true;
		}
		
		
		HashMap<String, Long> edges = new HashMap<String, Long>();
		
		while((line = reader.readLine()) != null) // while not EOF
		{
			
			String[] parts = line.split("\t");
			
			String start = "";
			String end   = "";
			long   count = 0;
			
			try{
				start = parts[0];
				end   = parts[1];
				count = Long.parseLong(parts[2]);
			}
			catch(ArrayIndexOutOfBoundsException e)
			{
				continue; //Eintrag ohne Name -> ID ist -1
			}
			
			String resultStart = map.get(start);
			String resultEnd   = map.get(end);

			//Kanten in Kategorien einteilen für die Darstellung. Abhängig von den Häufigkeiten der Kanten
			int choosenWriter = -1;
			if(count > 1500000) { choosenWriter = 0; } else
			if(count > 1000000) { choosenWriter = 1; } else
			if(count >  500000) { choosenWriter = 2; } else
			if(count >  100000) { choosenWriter = 3; } else
			if(count >   50000) { choosenWriter = 4; } else
			if(count >   10000) { choosenWriter = 5; } else
			if(count >    1000) { choosenWriter = 6; } else
			if(count >     100) { choosenWriter = 7; } else
			                    { choosenWriter = 8; }
			
			// -----------------------------------------
			
			//Kante würde eh nicht angezeigt werden sollen, daher gleich verwerfen
			if (choosenWriter > maxEdgeLevel) {
				skipped++;
				continue;
			}
			
			//Keine sinnvollen Koordinaten ("Null Island")
			if(resultStart.equals("0.0,0.0,0") || resultEnd.equals("0.0,0.0,0")) 
			{			
				skipped++;
				continue;
			}

			//Eintrag ist nicht vollständig, eventuell Kanten zwischen Orten zu denen keine Glyphen entstehen würden -> Sinnlos und weg
			if(resultStart.length() == 0 || resultEnd.length() == 0)
			{
			//	System.out.println("Not in XML - skip");
				skipped++;
				continue;
			}
			

			if(count > countMax)
			{
				countMax = count;
			}
			
			parts = resultStart.split(",");
			resultStart = parts[0]+","+parts[1];
			parts = resultEnd.split(",");
			resultEnd = parts[0]+","+parts[1];

			//Map - Kanten bestimmen und nur dann übernehmen, wenn sie kein unnötiges Duplikat ist
			
			String tmpStart[] = resultStart.split(",");
			String tmpEnd[]   =   resultEnd.split(",");

			double lonStart = Double.parseDouble(tmpStart[0]);
			double lonEnd   =   Double.parseDouble(tmpEnd[0]);
			
			String key  = "";

			//Normalisierter Key für die Map erstellen
			if(lonStart == lonEnd)
			{
				double latStart = Double.parseDouble(tmpStart[1]);
				double latEnd   =   Double.parseDouble(tmpEnd[1]);
				
				if(latStart > latEnd)
				{
					key = resultEnd+","+resultStart;
				}
				else
				{
					key = resultStart+","+resultEnd;
				}
				
			}
			else if(lonStart > lonEnd)
			{
				key = resultStart+","+resultEnd;
			}
			else
			{
				key = resultEnd+","+resultStart;
			}
			
			
			
			if(edges.get(key) != null)
			{	
				if(count > edges.get(key)) //Aktueller Eintrag ist schon vorhanden, aber jetzt schwere Kante, nehme die aktuelle mit dazu (wird durch Layerplatzierung nach der schwachen gemalt -> oben
											//Sollte nicht auftreten, aber da "United States" und "United States of America" beides vorkommt und einzelne Landsglyphe wird -> Gleiche Kante mit verschiedenen Gewichten...
				{
					edges.put(key, count);
					
					if(!initial[choosenWriter])
					{
						writers[choosenWriter].append(",\n");
					}
					else
					{
						initial[choosenWriter] = false;
					}
					
					writers[choosenWriter].append("[["  + resultStart + "],[" + resultEnd + "]]");
					
					numberOfEdges++;
				}
				else
				{
					duplicate++;
				}
			}
			else
			{
				edges.put(key, count);
			
				if(!initial[choosenWriter])
				{
					writers[choosenWriter].append(",\n");
				}
				else
				{
					initial[choosenWriter] = false;
				}
				
				writers[choosenWriter].append("[["  + resultStart + "],[" + resultEnd + "]]");
				
				numberOfEdges++;
			}
		}
		
				
		writeOnAll("];\n\n\n");
		
		writers[0].append("var glyphSize = " + glyphSize + ";\n");
		
		// schließe den BufferedReader
		reader.close();
	}
	
	/**
	 * Schreibt in jeden der Writer den gleichen Text hinten dran.
	 * @param input
	 * 		Text, der in alle Writer muss
	 * @throws IOException
	 */
	private static void writeOnAll(String input) throws IOException
	{
		for(int i = 0; i < NUMBER_OF_WRITER; i++)
		{
			writers[i].append(input);
		}
	}

	//Mapping zwischen der affiliationID und den Koordinaten erstellen
	//Dafür mittels XPath in XML die Daten raussuchen und in Map abspeichern.
	private static HashMap<String, String> generateMappingAffIDToCoords(String path, String xml) throws IOException, XPathExpressionException, ParserConfigurationException, SAXException
	{
		BufferedReader reader = new BufferedReader(new FileReader(path));

		reader.readLine();
		reader.readLine();
		
		String line = "";
		String[] parts;
		
		HashMap<String, String> map = new HashMap<String, String>();
		
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(xml);
		
		XPathFactory xPathFactory = XPathFactory.newInstance();
		XPath xpath = xPathFactory.newXPath();
		XPathExpression expression;
		
		
		while((line = reader.readLine()) != null)
		{
			parts = line.split("\t");
			
			expression = xpath.compile("//Placemark[@id='"+parts[2]+"']/Point/coordinates");
			String coords = expression.evaluate(doc);
			
			
			map.put(parts[1], coords);		
		}
		
		reader.close();
		return map;
	}
	
	//Selbiges wie oben auf Länderebene
	private static HashMap<String, String> generateMappingCountryToCoords(String path, String xml) throws IOException, XPathExpressionException, ParserConfigurationException, SAXException
	{
		BufferedReader reader = new BufferedReader(new FileReader(path));

		reader.readLine();
		reader.readLine();
		
		String line = "";
		String[] parts;
		
		HashMap<String, String> map = new HashMap<String, String>();
		
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(xml);
		
		XPathFactory xPathFactory = XPathFactory.newInstance();
		XPath xpath = xPathFactory.newXPath();
		XPathExpression expression;
		
		
		while((line = reader.readLine()) != null)
		{
			parts = line.split("\t");
			
			expression = xpath.compile("//Placemark[@id='"+parts[1]+"']/Point/coordinates");
			String coords = expression.evaluate(doc);
			
			
			map.put(parts[1], coords);		
		}
		
		reader.close();
		return map;
	}
	
	/**
	 * Initialisiert alle FileWriter.
	 * @param number
	 * 		Anzahl der Writer
	 * @throws IOException
	 */
	private static void initializeWriters(int number) throws IOException
	{
		for(int i = 0; i < number; i++)
		{
			writers[i] = new FileWriter(new File("./Visualisierung/KantenListen/coauthorships_output"+i+".txt"));
		}
	}
	
	/**
	 * Schließt alle FileWriter.
	 * @throws IOException
	 */
	private static void closeWriters() throws IOException
	{
		for(int i = 0; i < NUMBER_OF_WRITER; i++)
		{
			writers[i].close();
		}
	}
	
	/**
	 * Aufruf zum Erzeugen der Kanten für die Darstellung.
	 * @param pathAff		Affiliations-File
	 * @param pathInputXML	XML mit Geo-Koordinaten
	 * @param pathCoAuthors	materialisierte Ko-Autorschaften
	 * @param glyphSize		zusätzliche Größe für zusammengefasste Ortsglyphen
	 * @param maxEdgeLevel	nur Kanten mit Level < maxEdgeLevel werden dargestellt
	 * @param countryLevel	boolean, falls true werden Länder als Orte verwendet, sonst direkt Affiliations
	 */
	public static void initializeMapCoauthorships(String pathAff, String pathInputXML, String pathCoAuthors, int glyphSize, int maxEdgeLevel, boolean countryLevel)
	{
		try {
			initializeWriters(NUMBER_OF_WRITER);
			
			HashMap<String, String> map = null;
			
			if (countryLevel) {
				map = generateMappingCountryToCoords(pathAff, pathInputXML);	
			} else {
				map = generateMappingAffIDToCoords(pathAff, pathInputXML);		
			}
			
			getCoauthorships(pathCoAuthors, map, glyphSize, maxEdgeLevel);
			
			closeWriters();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		System.out.println("CoAuthroship Edges - > Done (" + numberOfEdges + " / " + skipped + " / "+ duplicate + ")");
		System.out.println("Heaviest Edge: " + countMax);

	}
	
}
