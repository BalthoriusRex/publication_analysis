package de.bigdprak.ss2016.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.bigdprak.ss2016.SimpleApp;



public class RandomAccessFileCoordinateWriter {

	
	
	public static RandomAccessFile	rafFile;
	public static long				rafOffset = 0;
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
			String tmp = "";
			rafFile = new RandomAccessFile(path, "rw");
			for(int i = 0; i < offset; i++)
			{
				tmp = rafFile.readLine();
				linesRead++;
			}
			System.out.println("Offset zuende gegangen");			
		}
		catch(IOException e)
		{
			System.out.println("File not found");
			e.printStackTrace();
		}
		
		
		
		System.out.println("RAF initialisiert");
	}
	
	public static void closeReader()
	{
		
		System.out.println("END: ----- Read " + linesRead + " lines. Use this as new offset!");
		try
		{
			rafFile.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			System.out.println("Reader could not be closed!");
		}
	}
	
	/**
	 * Get the Name of the next Affiliation
	 * @return Name of Affiliation
	 */
	public static String readNextAffiliation()
	{
		System.out.println("Read next Affiliation");
		String newLine = "";
		try
		{
			//Searching
			while(!(newLine.contains(SimpleApp.TAG_AFFILIATION_FULLNAME)))
			{
				newLine = rafFile.readLine();
				linesRead++;
				if(newLine == null)
				{
					return null;
				}
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
	
	public static void writeCoords(double lng, double lat)
	{
		System.out.println("WriteCoordinates");
		try
		{
			String tmp = "";
			while(!(tmp.contains("<coordinates>")))
			{
				tmp = rafFile.readLine();
			}
			
			rafFile.seek(rafFile.getFilePointer()-tmp.length());

			Character tmpChar = '-';
			while(tmpChar != '>')
			{
				tmpChar = rafFile.readChar();
			}
			
			byte[] coords = (lng+","+lat+",0").getBytes();
			
			rafFile.write(coords);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	
}
