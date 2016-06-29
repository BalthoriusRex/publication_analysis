package de.bigdprak.ss2016.database;

public class Affiliation {

	private long affiliationID;
	private String affiliationName;
	
	public Affiliation(String[] parts) {
		this.affiliationID = Long.parseLong(parts[0], 16);
		this.affiliationName = parts[1];
	}

	public long getAffiliationID() {
		return affiliationID;
	}

	public String getAffiliationName() {
		return affiliationName;
	}
}
