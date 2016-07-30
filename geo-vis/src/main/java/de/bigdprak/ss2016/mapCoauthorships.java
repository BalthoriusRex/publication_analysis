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

public class mapCoauthorships {

	public static final int NUMBER_OF_WRITER = 10;
	


	public static FileWriter[] writers = new FileWriter[NUMBER_OF_WRITER];
//	public static FileWriter fileWriter;
	
	public static int numberOfEdges = 0;
	public static long countMax = 0;
	
	
	
	public static void getCoauthorships(String authorships, HashMap<String, String> map) throws IOException, XPathExpressionException, SAXException, ParserConfigurationException
	{
		//Entferne KML-Tags vor Nutzung des XMLs!
		BufferedReader reader = new BufferedReader(new FileReader(new File(authorships)));
		
		String line;
		reader.readLine();	//Initialzeilen mit Querybeschreibung
		reader.readLine();
		
		//String kanten = "[";
		//fileWriter.append("var coauthorships = [");
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
		
		Vector<Long> counts = new Vector<Long>();
		
		int presentLineNumber = 0;
		
		while((line = reader.readLine()) != null)
		{
			if(presentLineNumber%5000 == 0)
			{
				System.out.println("Line #"+presentLineNumber + " done");
			}
			presentLineNumber++;
			
			
			String[] parts = line.split("\t");
			
			String start = "";
			String end   = "";
			long   count = 0;
			
			try{
				start = parts[0];
				end   = parts[1];
				count = Long.parseLong(parts[2]);
				
				if(count > countMax)
				{
					countMax = count;
				}
			}
			catch(ArrayIndexOutOfBoundsException e)
			{
				continue; //Eintrag ohne Name -> ID ist -1
			}


			String resultStart = map.get(start);
			String resultEnd   = map.get(end);
		//	System.out.println(end + " " + resultStart + " " + resultEnd);

			int choosenWriter = -1;
			if(count > 1500000)
			{
				choosenWriter = 0;
				//continue;
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
			} else if(count > 10)
			{
				choosenWriter = 8;
			} else
			{
				choosenWriter = 9;
			}
			
			
			if(!(resultStart.equals("0.0,0.0,0") || resultEnd.equals("0.0,0.0,0")))
			{			

				
				parts = resultStart.split(",");
				resultStart = parts[0]+","+parts[1];
				parts = resultEnd.split(",");
				resultEnd = parts[0]+","+parts[1];
				
				if(!initial[choosenWriter])
				{
				//	kanten += ",";
					writers[choosenWriter].append(",\n");
				}
				else
				{
					initial[choosenWriter] = false;
				}
				/*
				String kante = "[[" + resultStart
					   	   + "],[" + resultEnd
						   + "]]";
				kanten += kante;
				*/
				/*fileWriter.append("[["  + resultStart
					   	        + "],[" + resultEnd
						        + "]]");
				*/
				writers[choosenWriter].append(
						"[["  + resultStart
			   	        + "],[" + resultEnd
				        + "]]");
				numberOfEdges++;
				
			//	counts.addElement(count);
			}	

		}
		
		//kanten += "]";
		writeOnAll("];\n\n\n");//var anzahl = [" + counts.toString() + "];");
		
		reader.close();
		//String[] returnArray = {kanten, counts.toString()};
		//return returnArray; 
	}
	
	public static void writeOnAll(String input) throws IOException
	{
		for(int i = 0; i < NUMBER_OF_WRITER; i++)
		{
			writers[i].append(input);
		}
	}

	public static HashMap<String, String> generateMappingIDNormalizedAffiliationName(String path, String xml) throws IOException, XPathExpressionException, ParserConfigurationException, SAXException
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
	
	public static void main(String args[])
	{
		try {
			initializeWriters(NUMBER_OF_WRITER);
			//fileWriter = new FileWriter(new File("./Visualisierung/coauthorships_output.txt"));
		
			HashMap<String, String> map = generateMappingIDNormalizedAffiliationName("./Visualisierung/affiliations_top_1000.txt", "./Visualisierung/Xml/testMapping.xml");
		//	writeCoauthorships(getCoauthorships("./Visualisierung/coauthorships_complete_reduced.txt", map));
			getCoauthorships("./Visualisierung/coauthorships_complete_top_100.txt", map);
			
		//	fileWriter.close();
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
	}
}
