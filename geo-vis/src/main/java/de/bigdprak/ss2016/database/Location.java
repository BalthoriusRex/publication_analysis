package de.bigdprak.ss2016.database;

/**
 * Die Location-Klasse dient der Modellierung der durch Geocoding erlangten Informationen
 * zu Orten bzw. Affiliations. 
 */
public class Location {

	private long id;
	private String name;
	private String country;
	private double longitude;
	private double latitude;
	
	public Location(long id, String name, String country, double longitude,
			double latitude) {
		super();
		this.id = id;
		this.name = name;
		this.country = country;
		this.longitude = longitude;
		this.latitude = latitude;
	}
	
	public Location(String name, String country, double longitude,
			double latitude) {
		super();
		this.id = -1;
		this.name = name;
		this.country = country;
		this.longitude = longitude;
		this.latitude = latitude;
	}
	
	public long getId() {
		return id;
	}
	public String getName() {
		return name;
	}
	public String getCountry() {
		return country;
	}
	public double getLongitude() {
		return longitude;
	}
	public double getLatitude() {
		return latitude;
	}
}
