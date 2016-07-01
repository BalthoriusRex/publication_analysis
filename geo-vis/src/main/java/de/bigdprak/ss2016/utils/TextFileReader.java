package de.bigdprak.ss2016.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TextFileReader {
	
	/**
	 * Liest file und gibt in Datei enthaltenen Text zurück.
	 * @param file Dateimit hinterlegter Directory
	 * @return Dateiinhalt
	 */
	public static String read(File file){
		return read(file.getPath());
	}
	
	/**
	 * Liest directory und gibt in Datei enthaltenen Text zurück.
	 * @param directory Dateipfad
	 * @return Dateiinhalt
	 */
	public static String read(String directory){
		String fileText = "";
		try {
			@SuppressWarnings("resource")
			BufferedReader in = new BufferedReader(new FileReader(directory));
			String zeile = null;
			while ((zeile = in.readLine()) != null) {
				//System.out.println("Gelesene Zeile: " + zeile);
				if(!fileText.equals("")){
					fileText += "\r\n";
				}
				fileText += zeile;
			}
		} catch (FileNotFoundException e) {
			System.err.println("Exception: File not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("FileText: \n"+fileText);
		return fileText;
	}
	
	public static void main(String[] args){
		TextFileWriter.writeOver("/Users/Daniel/Documents/TestDomainFile.txt", "www.google.de");
		System.out.println(TextFileReader.read("/Users/Daniel/Documents/TestDomainFile.txt"));
	}
	
}