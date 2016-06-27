package de.bigdprak.ss2016.database;

public class Paper {

	private long paperID;
	private String originalPaperTitle;
	
	public Paper(String[] parts) {
		this.paperID = Long.parseLong(parts[0], 16);
		this.originalPaperTitle = parts[1];
	}
	
	public Paper(long paperID, String originalPaperTitle) {
		super();
		this.paperID = paperID;
		this.originalPaperTitle = originalPaperTitle;
	}

	public long getPaperID() {
		return paperID;
	}

	public String getOriginalPaperTitle() {
		return originalPaperTitle;
	}
}
