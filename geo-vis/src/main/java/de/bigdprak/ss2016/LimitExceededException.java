package de.bigdprak.ss2016;

@SuppressWarnings("serial")
public class LimitExceededException extends Exception {
	
	// TODO schreibe ein Protokoll, sodass das Programm beim nächsten mal an der selben Stelle forfahren kann.
	
	private int error;
	
	public final static int ERROR_LIMIT = 0;
	public final static int ERROR_EOF = 1;
	
	private final static String MSG_LIMIT = "Geocoding service offers no further requests... try again tomorrow.";
	private final static String MSG_EOF = "Reached end of file... Programm will stop... Computation done.";
	private final static String MSG_UNKNOWN_ERROR = "an unknown error occured...";
	
	public LimitExceededException(int error) {
		super();
		this.error = error;
	}
	
	public String getMessage() {
		String ret = null;
		switch (this.error) {
		case ERROR_LIMIT: {
			ret = MSG_LIMIT;
			break;
		}
		case ERROR_EOF: {
			ret = MSG_EOF;
			break;
		}
		default: {
			ret = MSG_UNKNOWN_ERROR;
		}
		}
		return ret;
	}
	
	public int getErrorCode() {
		return this.error;
	}
	
	
}
