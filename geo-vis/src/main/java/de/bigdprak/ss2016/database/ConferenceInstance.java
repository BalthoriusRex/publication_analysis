package de.bigdprak.ss2016.database;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class ConferenceInstance {
	
	private long conferenceSeriesID;
	private long conferenceInstanceID;
	private String shortName;
	private String fullName;
	private String location;
	private URL officialConferenceURL;
	private Calendar conferenceStartDate;
	private Calendar conferenceAbstractRegistrationDate;
	private Calendar conferenceSubmissionDeadlineDate;
	private Calendar conferenceNotificationDueDate;
	private Calendar conferenceFinalVersionDueDate;
	
	public ConferenceInstance(String[] parts) {
		this.conferenceSeriesID = Long.parseLong(parts[0], 16);
		this.conferenceInstanceID = Long.parseLong(parts[1], 16);
		this.shortName = parts[2];
		this.fullName = parts[3];
		this.location = parts[4];
		try {
			this.officialConferenceURL = new URL(parts[5]);
		} catch (MalformedURLException e) {
			//e.printStackTrace();
			System.err.println("Error while parsing URL for ConferenceInstance");
			System.err.println("Input: " + parts[5]);
			System.err.println(e.getMessage());
			this.officialConferenceURL = null;
		}
		this.conferenceStartDate = parseDate(parts[6]);
		this.conferenceAbstractRegistrationDate = parseDate(parts[7]);
		this.conferenceSubmissionDeadlineDate = parseDate(parts[8]);
		this.conferenceNotificationDueDate = parseDate(parts[9]);
		this.conferenceFinalVersionDueDate = parseDate(parts[10]);
	}
	
	private Calendar parseDate(String dateString) {
		Calendar date = null;
		
		String[] parts = dateString.split("/");
		int year = Integer.parseInt(parts[0]);
		int month = Integer.parseInt(parts[1]);
		int day = Integer.parseInt(parts[2]);
		
		date = new GregorianCalendar(year, month, day);
		
		return date;
		
	}

	public long getConferenceSeriesID() {
		return conferenceSeriesID;
	}

	public long getConferenceInstanceID() {
		return conferenceInstanceID;
	}

	public String getShortName() {
		return shortName;
	}

	public String getFullName() {
		return fullName;
	}

	public String getLocation() {
		return location;
	}

	public URL getOfficialConferenceURL() {
		return officialConferenceURL;
	}

	public Calendar getConferenceStartDate() {
		return conferenceStartDate;
	}

	public Calendar getConferenceAbstractRegistrationDate() {
		return conferenceAbstractRegistrationDate;
	}

	public Calendar getConferenceSubmissionDeadlineDate() {
		return conferenceSubmissionDeadlineDate;
	}

	public Calendar getConferenceNotificationDueDate() {
		return conferenceNotificationDueDate;
	}

	public Calendar getConferenceFinalVersionDueDate() {
		return conferenceFinalVersionDueDate;
	}
}
