package de.bigdprak.ss2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class mapCoauthorships {

	
	public static String getCoauthorships(String authorships, String xml) throws IOException, XPathExpressionException, SAXException, ParserConfigurationException
	{
		//Entferne KML-Tags vor Nutzung des XMLs!
		BufferedReader reader = new BufferedReader(new FileReader(new File(authorships)));
		
		String line;
		reader.readLine();	//Initialzeilen mit Querybeschreibung
		reader.readLine();
		
		String kanten = "[";
		
		boolean initial = true;
		
		while((line = reader.readLine()) != null)
		{
			if(!initial)
			{
				kanten += ",";
			}
			
			initial = false;
			
			//System.out.println(line);
			String[] parts = line.split("\t");
			
			String start = "";
			String end   = "";
			
			try{
				start = parts[3];
				end   = parts[4];
			}
			catch(ArrayIndexOutOfBoundsException e)
			{
				continue; //Eintrag ohne Name -> ID ist -1
			}
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setNamespaceAware(true);
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(xml);
			
			XPathFactory xPathFactory = XPathFactory.newInstance();
			XPath xpath = xPathFactory.newXPath();
			
			XPathExpression expression = xpath.compile("//Placemark[@id='"+start+"']/Point/coordinates");
			String resultStart = expression.evaluate(doc);
			
			expression = xpath.compile("//Placemark[@id='"+end+"']/Point/coordinates");
			String resultEnd = expression.evaluate(doc);
			
			if(!(resultEnd.length() == 0))
			{
				parts = resultStart.split(",");
				resultStart = parts[0]+","+parts[1];
				parts = resultEnd.split(",");
				resultEnd = parts[0]+","+parts[1];
				
				String kante = "[[" + resultStart
					   	   + "],[" + resultEnd
						   + "]]";
				kanten += kante;
			}	
			//System.out.println(kante);
		}
		
		kanten += "]";
		
		return kanten; 
	}
	
	
	public static void main(String args[])
	{
		try {
			System.out.println(getCoauthorships("./Visualisierung/coauthorships.txt", "./Visualisierung/testMapping.xml"));

		} catch (XPathExpressionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
}
