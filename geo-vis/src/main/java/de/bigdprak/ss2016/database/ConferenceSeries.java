package de.bigdprak.ss2016.database;

public class ConferenceSeries {

	private long conferenceSeriesID;
	private String shortName;
	private String fullName;
	
	public ConferenceSeries(String[] parts) {
		this.conferenceSeriesID = Long.parseLong(parts[0], 16);
		this.shortName = parts[1];
		this.fullName = parts[2];
	}

	public long getConferenceSeriesID() {
		return conferenceSeriesID;
	}

	public String getShortName() {
		return shortName;
	}

	public String getFullName() {
		return fullName;
	}
}
