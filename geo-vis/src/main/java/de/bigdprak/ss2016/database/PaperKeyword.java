package de.bigdprak.ss2016.database;

public class PaperKeyword {

	private long paperID;
	private String keywordName;
	private long fieldOfStudyIDmappedToKeyword;
	
	public PaperKeyword(String[] parts) {
		this.paperID = Long.parseLong(parts[0], 16);
		this.keywordName = parts[1];
		this.fieldOfStudyIDmappedToKeyword = Long.parseLong(parts[2], 16);
	}

	public long getPaperID() {
		return paperID;
	}

	public String getKeywordName() {
		return keywordName;
	}

	public long getFieldOfStudyIDmappedToKeyword() {
		return fieldOfStudyIDmappedToKeyword;
	}
}
