package de.bigdprak.ss2016.database;

import java.net.MalformedURLException;
import java.net.URL;

public class PaperURL {

	private long paperID;
	private URL paperURL;
	
	public PaperURL(String[] parts) {
		this.paperID = Long.parseLong(parts[0], 16);
		try {
			this.paperURL = new URL(parts[1]);
		} catch (MalformedURLException e) {
			//e.printStackTrace();
			System.err.println("Error while parsing URL for PaperURL");
			System.err.println("Input: " + parts[1]);
			System.err.println(e.getMessage());
			this.paperURL = null;
		}
	}

	public long getPaperID() {
		return paperID;
	}

	public URL getPaperURL() {
		return paperURL;
	}
}
