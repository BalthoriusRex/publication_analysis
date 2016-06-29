package de.bigdprak.ss2016.database;

public class Journal {

	private long journalID;
	private String journalName;
	
	public Journal(String[] parts) {
		this.journalID = Long.parseLong(parts[0], 16);
		this.journalName = parts[1];
	}

	public long getJournalID() {
		return journalID;
	}

	public String getJournalName() {
		return journalName;
	}
}
