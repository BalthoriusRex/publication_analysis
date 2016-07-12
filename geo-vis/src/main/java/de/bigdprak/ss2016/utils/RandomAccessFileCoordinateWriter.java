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
	 * Initializing a reader for further use
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
	//	System.out.println("ENDE: ---- Alt: " + offsetOld + " Neu: " + entriesDone + " Neuer Offset: " + (offsetOld + entriesDone) +"  <- neuer Offset");
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
	 * @throws IOException 
	 */
	public static String readNext(int value) throws IOException
	{
		String start = "";
		String end 	 = "";
		switch (value) {
		case 1:
			start  	= "<Placemark";
			end		= "\">";
			break;
		case 2:
			start 	= SimpleApp.TAG_AFFILIATION_FULLNAME;
			end 	= "<\""+SimpleApp.TAG_AFFILIATION_FULLNAME+">";
		}
		
		entriesDone++;
		System.out.println("Read next Affiliation");
		String newLine = "";

		//Searching
		boolean stop = false;
		while (!stop)
		{
			newLine = rafFile.readLine();
			//System.out.println(tag + " vs. " + newLine);
			if (newLine == null) {
				stop = true;
				return null;
			} else {
				if (newLine.contains(start)) {
					stop = true;
				}
			}
			linesRead++;
		}
		
		//Parse innerHTML
		String[] parts = newLine.split(start);
		parts = parts[1].split(end);
		//Get Affiliation
		newLine = parts[0];
		return newLine;
	}
	
	public static String readNextNormalizedAffiliation() throws IOException {
		return readNext(1);
	}
	
	public static String readNextOriginalAffiliation() throws IOException {
		return readNext(2);
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
