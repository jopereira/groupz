package groupz;

/**
 * Group communication exception. Any exception within the group
 * communication protocol or the application callbacks removes
 * the process from the view.
 * 
 * @author jop
 */
public class GroupException extends Exception {
	GroupException(String message) {
		super(message);
	}

	GroupException(String message, Throwable cause) {
		super(message, cause);
	}

	private static final long serialVersionUID = 1L;
}
