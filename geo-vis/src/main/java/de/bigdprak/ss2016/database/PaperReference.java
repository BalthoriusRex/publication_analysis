package de.bigdprak.ss2016.database;

public class PaperReference {

	private long paperID;
	private long paperReferenceID;
	
	public PaperReference(String[] parts) {
		this.paperID = Long.parseLong(parts[0], 16);
		this.paperReferenceID = Long.parseLong(parts[1], 16);
	}

	public long getPaperID() {
		return paperID;
	}

	public long getPaperReferenceID() {
		return paperReferenceID;
	}
}
