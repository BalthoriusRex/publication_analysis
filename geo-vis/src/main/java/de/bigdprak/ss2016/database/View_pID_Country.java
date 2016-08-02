package de.bigdprak.ss2016.database;

public class View_pID_Country {

	private long paperID;
	private String countryName;
	
	public View_pID_Country(long paperID, String countryName) {
		super();
		this.paperID = paperID;
		this.countryName = countryName;
	}
	
	public long getPaperID() {
		return paperID;
	}
	public String getCountryName() {
		return countryName;
	}
	
	
}
