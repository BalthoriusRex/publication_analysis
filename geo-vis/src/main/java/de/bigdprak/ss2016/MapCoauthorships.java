package de.bigdprak.ss2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import de.bigdprak.ss2016.utils.TextFileWriter;

public class MapCoauthorships {

	public static final int NUMBER_OF_WRITER = 9;
	


	public static FileWriter[] writers = new FileWriter[NUMBER_OF_WRITER];
	
	public static int numberOfEdges = 0;
	public static long countMax = 0;
	
	public static int  skipped = 0;
	
	public static void getCoauthorshipsLocations(String authorships, HashMap<String, String> map, int glyphSize, int maxEdgeLevel) throws IOException, XPathExpressionException, SAXException, ParserConfigurationException
	{
		//Entferne KML-Tags vor Nutzung des XMLs!
		BufferedReader reader = new BufferedReader(new FileReader(new File(authorships)));
		
		String line;
		reader.readLine();	//Initialzeilen mit Querybeschreibung
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
		
		
		while((line = reader.readLine()) != null)
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

			int choosenWriter = -1;
			if(count > 1500000)
			{
				choosenWriter = 0;
			} else if(count > 1000000)
			{
				choosenWriter = 1;
			} else if(count > 500000)
			{
				choosenWriter = 2;
			} else if(count > 100000)
			{
				choosenWriter = 3;
			} else if(count > 50000)
			{
				choosenWriter = 4;
			} else if(count > 10000)
			{
				choosenWriter = 5;
			} else if(count > 1000)
			{
				choosenWriter = 6;
			} else if(count > 100)
			{
				choosenWriter = 7;
			}else
			{
				choosenWriter = 8;
			}
			
			// -----------------------------------------
			
			if (choosenWriter > maxEdgeLevel) {
				skipped++;
				continue;
			}
			
			if(resultStart.equals("0.0,0.0,0") || resultEnd.equals("0.0,0.0,0")) 
			{			
				skipped++;
				continue;
			}

			
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
			
			if(!initial[choosenWriter])
			{
				writers[choosenWriter].append(",\n");
			}
			else
			{
				initial[choosenWriter] = false;
			}

			writers[choosenWriter].append(
					"[["  + resultStart
		   	        + "],[" + resultEnd
			        + "]]");
			numberOfEdges++;				
				

		}
		
		writeOnAll("];\n\n\n");
		
		writers[0].append("var glyphSize = " + glyphSize + ";\n");
		
		reader.close();
	}
	
	public static void getCoauthorshipsCountries(String authorships, HashMap<String, String> map, int glyphSize, int maxEdgeLevel) throws IOException, XPathExpressionException, SAXException, ParserConfigurationException
	{
		//Entferne KML-Tags vor Nutzung des XMLs!
		BufferedReader reader = new BufferedReader(new FileReader(new File(authorships)));
		
		String line;
		reader.readLine();	//Initialzeilen mit Querybeschreibung
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
		
		
		while((line = reader.readLine()) != null)
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

			int choosenWriter = -1;
			if(count > 1500000)
			{
				choosenWriter = 0;
			} else if(count > 1000000)
			{
				choosenWriter = 1;
			} else if(count > 500000)
			{
				choosenWriter = 2;
			} else if(count > 100000)
			{
				choosenWriter = 3;
			} else if(count > 50000)
			{
				choosenWriter = 4;
			} else if(count > 10000)
			{
				choosenWriter = 5;
			} else if(count > 1000)
			{
				choosenWriter = 6;
			} else if(count > 100)
			{
				choosenWriter = 7;
			}else
			{
				choosenWriter = 8;
			}
			
			// -----------------------------------------
			
			if (choosenWriter > maxEdgeLevel) {
				skipped++;
				continue;
			}
			
			if(resultStart.equals("0.0,0.0,0") || resultEnd.equals("0.0,0.0,0")) 
			{			
				skipped++;
				continue;
			}

			
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
			
			if(!initial[choosenWriter])
			{
				writers[choosenWriter].append(",\n");
			}
			else
			{
				initial[choosenWriter] = false;
			}

			writers[choosenWriter].append(
					"[["  + resultStart
		   	        + "],[" + resultEnd
			        + "]]");
			numberOfEdges++;				
				

		}
		
		writeOnAll("];\n\n\n");
		
		writers[0].append("var glyphSize = " + glyphSize + ";\n");
		
		reader.close();
	}
	
	public static void writeOnAll(String input) throws IOException
	{
		for(int i = 0; i < NUMBER_OF_WRITER; i++)
		{
			writers[i].append(input);
		}
	}

	public static HashMap<String, String> generateMappingAffIDToCoords(String path, String xml) throws IOException, XPathExpressionException, ParserConfigurationException, SAXException
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
	
	public static HashMap<String, String> generateMappingCountryToCoords(String path, String xml) throws IOException, XPathExpressionException, ParserConfigurationException, SAXException
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
	
	public static void initializeWriters(int number) throws IOException
	{
		for(int i = 0; i < number; i++)
		{
			writers[i] = new FileWriter(new File("./Visualisierung/KantenListen/coauthorships_output"+i+".txt"));
		}
	}
	
	public static void closeWriters() throws IOException
	{
		for(int i = 0; i < NUMBER_OF_WRITER; i++)
		{
			writers[i].close();
		}
	}
	
	/*
	public static void main(String args[])
	{
		try {
			initializeWriters(NUMBER_OF_WRITER);
		
			HashMap<String, String> map = generateMappingIDNormalizedAffiliationName("./Visualisierung/affiliations_top_1000.txt", "./Visualisierung/Karten/Xml/mapCoauthorship_input.xml");
			getCoauthorships("./Visualisierung/coauthorships_complete_reduced.txt", map);
			
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
		System.out.println("Done (" + numberOfEdges + ")");
		System.out.println("CountMax: " + countMax);
	}*/
	
	public static void initializeMapCoauthorships(String pathAff, String pathInputXML, String pathCoAuthors, int glyphSize, int maxEdgeLevel, boolean countryLevel)
	{
		try {
			initializeWriters(NUMBER_OF_WRITER);
			
			if (countryLevel) {
				HashMap<String, String> map = generateMappingCountryToCoords(pathAff, pathInputXML);			
				getCoauthorshipsCountries(pathCoAuthors, map, glyphSize, maxEdgeLevel);
			} else {
				HashMap<String, String> map = generateMappingAffIDToCoords(pathAff, pathInputXML);			
				getCoauthorshipsLocations(pathCoAuthors, map, glyphSize, maxEdgeLevel);
			}
			
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
		System.out.println("CoAuthroship Edges - > Done (" + numberOfEdges + " / "+skipped+")");
		System.out.println("Heaviest Edge: " + countMax);

	}
	
}
