package de.bigdprak.ss2016.database;

public class View_pID_affID_affName {

	public long paperID;
	public long affiliationID;
	public String normalizedAffiliationName;
	
	public View_pID_affID_affName(String[] parts) {
		this.paperID = Long.parseLong(parts[0], 16);
		this.affiliationID = parts[2].isEmpty() ? -1 : Long.parseLong(parts[2], 16);
		this.normalizedAffiliationName = parts[4];
	}

	public long getPaperID() {
		return paperID;
	}

	public long getAffiliationID() {
		return affiliationID;
	}

	public String getNormalizedAffiliationName() {
		return normalizedAffiliationName;
	}
	
	
}
