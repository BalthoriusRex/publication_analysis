package de.bigdprak.ss2016;

/**
 * Custom-Exception für das Geocoding.
 */
@SuppressWarnings("serial")
public class LimitExceededException extends Exception {
	
	// Implementierung ist obsolet, da ihre Inhalte nicht länger verwendet werden. 
	
	private int error;
	
	public final static int ERROR_LIMIT = 0;
	public final static int ERROR_EOF = 1;
	
	private final static String MSG_LIMIT = ""
							+ "Geocoding service offers no further requests... \n"
							+ "no more support for this user for today \n"
							+ "consider using another user key!\n";
	private final static String MSG_EOF = "Reached end of file... Programm will stop... Computation done.";
	private final static String MSG_UNKNOWN_ERROR = "an unknown error occured...";
	
	/**
	 * Konstruktor zur Verwendung mit einem der vorgegebenen Fehlercodes.
	 * @param error
	 * 		ERROR_LIMIT oder ERROR_EOF
	 */
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
