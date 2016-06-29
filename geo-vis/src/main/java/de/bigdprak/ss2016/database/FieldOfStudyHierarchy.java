package de.bigdprak.ss2016.database;

public class FieldOfStudyHierarchy {

	private long childFieldOfStudyID;
	private int childFieldOfStudyLevel; // L0, L1, L2, ... -> 0, 1, 2, ...
	private long parentFieldOfStudyID;
	private int parentFieldOfStudyLevel;
	private double confidence;
	
	public FieldOfStudyHierarchy(String[] parts) {
		this.childFieldOfStudyID = Long.parseLong(parts[0], 16);
		this.childFieldOfStudyLevel = Integer.parseInt(parts[1].substring(1));
		this.parentFieldOfStudyID = Long.parseLong(parts[2], 16);
		this.parentFieldOfStudyLevel = Integer.parseInt(parts[3].substring(1));
		this.confidence = Double.parseDouble(parts[4]);
	}

	public long getChildFieldOfStudyID() {
		return childFieldOfStudyID;
	}

	public int getChildFieldOfStudyLevel() {
		return childFieldOfStudyLevel;
	}

	public long getParentFieldOfStudyID() {
		return parentFieldOfStudyID;
	}

	public int getParentFieldOfStudyLevel() {
		return parentFieldOfStudyLevel;
	}

	public double getConfidence() {
		return confidence;
	}
}
