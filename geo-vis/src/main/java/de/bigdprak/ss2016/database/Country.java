package de.bigdprak.ss2016.database;

/**
 * Die Country-Klasse dient der Modellierung der durch Geocoding erlangten Informationen
 * zu Ländern. 
 */
public class Country {
	
	private String name;
	private String continent;
	private double longitude;
	private double latitude;
	
	public Country(String name, String continent, double longitude,
			double latitude) {
		super();
		this.name = name;
		this.continent = continent;
		this.longitude = longitude;
		this.latitude = latitude;
	}
	
	public String getName() {
		return name;
	}
	public String getContinent() {
		return continent;
	}
	public double getLongitude() {
		return longitude;
	}
	public double getLatitude() {
		return latitude;
	}
	
	public String toString() {
		return "" + name + " [" + continent + "]";
	}
	
	
}
