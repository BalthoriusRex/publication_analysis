package de.bigdprak.ss2016.database;

public class Author {
	private static long ID = 0;
	
	public long authorID;
	public String name;
	
	public Author(String name) {
		this.authorID = ID;
		this.name = name;
		ID++;
	}
	
	public Author(String ID, String name) {
		this.authorID = Long.parseLong(ID, 16);
		this.name = name;
	}
	
	public Author(long authorID, String name) {
		super();
		this.authorID = authorID;
		this.name = name;
	}

	public long getAuthorID() {
		return authorID;
	}

	public void setAuthorID(long authorID) {
		this.authorID = authorID;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
