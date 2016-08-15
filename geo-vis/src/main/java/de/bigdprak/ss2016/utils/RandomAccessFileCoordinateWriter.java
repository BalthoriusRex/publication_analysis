package de.bigdprak.ss2016.utils;

import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * Diese Klasse stellt Methoden zum Auslesen und Beschreiben von Dateien bereit.
 * Insbesondere wird diese Klasse zum Auslesen von Affiliationnamen aus XML-Dateien verwendet.
 */
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
	 * @return Name of Affiliation or null at EOF
	 * @throws IOException 
	 */
	private static String readNext(int value) throws IOException
	{
		String start = "";
		String end 	 = "";
		switch (value) {
		case 1:
			start  	= "<Placemark id='";
			end		= "'>";
			break;
		case 2:
			start 	= "<name>";
			end 	= "</name>";
		}
		
		entriesDone++;
		System.out.println("Read next Affiliation");
		String newLine = "";

		// springe zur nächsten Zeile, die einen Affiliationname enthält
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
		
		// entferne überschüssige XML-Tags
		String[] parts = newLine.split(start);
		parts = parts[1].split(end);
		// lies Affiliation
		String aff = parts[0];
		return aff;
	}
	
	/**
	 * Liest die Affiliation aus der nächsten Placemark-Zeile aus.
	 * @return Affiliationname oder null (bei EOF)
	 * @throws IOException
	 */
	public static String readNextNormalizedAffiliation() throws IOException {
		return readNext(1);
	}
	
	/**
	 * Liest die Affiliation aus der nächsten Name-Zeile aus.
	 * @return Affiliationname oder null (bei EOF)
	 * @throws IOException
	 */
	public static String readNextOriginalAffiliation() throws IOException {
		return readNext(2);
	}
	
	/**
	 * Schreibt Koordinaten an die Stelle in der Datei, die vorher durch Whitespace reserviert wurde.
	 * Dadurch werden keine "wichtigen" Zeichen überschrieben, sondern lediglich Whitespace aufgefüllt.
	 * @param lng Longitude
	 * @param lat Latitude
	 */
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
