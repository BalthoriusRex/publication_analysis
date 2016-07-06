package de.bigdprak.ss2016.utils;

import java.io.IOException;
import java.io.RandomAccessFile;

import de.bigdprak.ss2016.SimpleApp;



public class RandomAccessFileCoordinateWriter {

	
	
	public static RandomAccessFile	rafFile;
	public static long				rafOffset   = 0;
	public static int 				linesRead   = 0;
	public static int				entriesDone = 0;
	public static int				offsetOld   = 0;
	
	/**
	 * Initilizing a reader for further use
	 * @param path Path to the file
	 * @param offsetInEntries Number of initial lines to skip. Needed by limitation of geocaching-services
	 */
	public static void initializeReader(String path, int offsetInEntries)
	{
		try
		{
			offsetOld = offsetInEntries;
			int offsetInLines = offsetInEntries * 8 + 2;
			rafFile = new RandomAccessFile(path, "rw");
			for(int i = 0; i < offsetInLines; i++)
			{
				rafFile.readLine();
				linesRead++;
			}
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
		
	//	System.out.println("END: ----- Read " + linesRead + " lines. Offset was: " + rafOffset + " lines. Use this: " + (linesRead+rafOffset) + " as new offset!");
		System.out.println("ENDE: ---- Alt: " + offsetOld + " Neu: " + entriesDone + " Neuer Offset: " + (offsetOld + entriesDone) +"  <- neuer Offset");
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
		entriesDone++;
		System.out.println("Read next Affiliation");
		String newLine = "";
		try
		{
			//Searching
			while(!(newLine.contains(SimpleApp.TAG_AFFILIATION_NORMALIZED)))
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
			System.err.println("Error in readNextAffiliation");
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
			while(!(tmp.contains("<Point>")))
			{
				tmp = rafFile.readLine();
			}
			
		//	rafFile.seek(rafFile.getFilePointer() - tmp.length());

			String coords = "\t\t\t\t<coordinates>"+lng+","+lat+",0</coordinates>";
			rafFile.writeBytes(coords);
		}
		catch(IOException e)
		{
			System.err.println("Error in writeCoordinates");
			e.printStackTrace();
		}
	}
	
}
