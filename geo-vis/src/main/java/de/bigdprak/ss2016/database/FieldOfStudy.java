package de.bigdprak.ss2016.database;

public class FieldOfStudy {

	private long fieldOfStudyID;
	private String fieldOfStudyName;
	
	public FieldOfStudy(String[] parts) {
		this.fieldOfStudyID = Long.parseLong(parts[0], 16);
		this.fieldOfStudyName = parts[1];
	}

	public long getFieldOfStudyID() {
		return fieldOfStudyID;
	}

	public String getFieldOfStudyName() {
		return fieldOfStudyName;
	}
}
