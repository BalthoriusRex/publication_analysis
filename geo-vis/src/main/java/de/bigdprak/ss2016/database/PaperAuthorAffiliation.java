package de.bigdprak.ss2016.database;

public class PaperAuthorAffiliation {

	private long paperID;
	private long authorID;
	private long affiliationID;
	private String originalAffiliationName;
	private String normalizedAffiliationName;
	private int authorSequenceNumber;
	
	public PaperAuthorAffiliation(String[] parts) {
		super();
		this.paperID = Long.parseLong(parts[0], 16);
		this.authorID = Long.parseLong(parts[1], 16);
		this.affiliationID = parts[2].isEmpty() ? -1 : Long.parseLong(parts[2], 16);
		this.originalAffiliationName = parts[3];
		this.normalizedAffiliationName = parts[4];
		this.authorSequenceNumber = parts[5].isEmpty() ? -1 : Integer.parseInt(parts[5]);
	}
	
	public PaperAuthorAffiliation(String paperID, String authorID,
			String affiliationID, String originalAffiliationName,
			String normalizedAffiliationName, String authorSequenceNumber) {
		super();
		this.paperID = Long.parseLong(paperID, 16);
		this.authorID = Long.parseLong(authorID, 16);
		this.affiliationID = affiliationID.isEmpty() ? -1 : Long.parseLong(affiliationID, 16);
		this.originalAffiliationName = originalAffiliationName;
		this.normalizedAffiliationName = normalizedAffiliationName;
		this.authorSequenceNumber = authorSequenceNumber.isEmpty() ? -1 : Integer.parseInt(authorSequenceNumber);
	}
	
	public PaperAuthorAffiliation(long paperID, long authorID,
			long affiliationID, String originalAffiliationName,
			String normalizedAffiliationName, int authorSequenceNumber) {
		super();
		this.paperID = paperID;
		this.authorID = authorID;
		this.affiliationID = affiliationID;
		this.originalAffiliationName = originalAffiliationName;
		this.normalizedAffiliationName = normalizedAffiliationName;
		this.authorSequenceNumber = authorSequenceNumber;
	}

	public long getPaperID() {
		return paperID;
	}

	public long getAuthorID() {
		return authorID;
	}

	public long getAffiliationID() {
		return affiliationID;
	}

	public String getOriginalAffiliationName() {
		return originalAffiliationName;
	}

	public String getNormalizedAffiliationName() {
		return normalizedAffiliationName;
	}

	public int getAuthorSequenceNumber() {
		return authorSequenceNumber;
	}
}
