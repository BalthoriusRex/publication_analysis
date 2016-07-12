package de.bigdprak.ss2016.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TextFileReader {

	
	public static BufferedReader	reader;
	public static int 				linesRead = 0;
	
	/**
	 * Initilizing a reader for further use
	 * @param path Path to the file
	 * @param offset Number of initial lines to skip. Needed by limitation of geocaching-services
	 */
	public static void initializeReader(String path, int offset)
	{
		try
		{
			reader = new BufferedReader(new FileReader(path));
			for(int i = 0; i < offset; i++)
			{
				reader.readLine();
				linesRead++;
			}
		}
		catch(IOException e)
		{
			System.out.println("File not found");
			e.printStackTrace();
		}
	}
	
	public static void closeReader()
	{
		
		//System.out.println("END: ----- Read " + linesRead + " lines. Use this as new offset!");
		try
		{
			reader.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			System.out.println("Reader could not be closed!");
		}
	}
	
	public static int parseOffset(String path) {
		
		int offset = -1;
		
		try {
			BufferedReader r = new BufferedReader(new FileReader(path));
			String firstLine = r.readLine();
			r.close();
			offset = Integer.parseInt(firstLine);
		} catch (FileNotFoundException e) {
			offset = 0;
		} catch (IOException e) {
			offset = 0;
		} catch (NumberFormatException e) {
			offset = 0;
		}
		
		return offset;
	}
	
	/**
	 * Get the Name of the next Affiliation
	 * @return Name of Affiliation
	 */
	public static String readNextAffiliation()
	{
		String newLine = "";
		try
		{
			//Searching
			while(!(newLine.contains("<affiliation>")))
			{
				newLine = reader.readLine();
				linesRead++;
			}
			
			//Parse innerHTML
			String[] parts = newLine.split(">");
			parts = parts[1].split("<");
			//Get Affiliation
			newLine = parts[0];
		}
		catch(IOException e)
		{
			e.printStackTrace();
			return "---------------------------Error-----";
		}
		
		return newLine;
	}
	
	public static int getLinesRead()
	{
		return linesRead;
	}
	
}