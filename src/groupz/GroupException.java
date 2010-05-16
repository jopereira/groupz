package groupz;

import java.io.IOException;

/**
 * Group communication exception. Any exception within the group
 * communication protocol or the application callbacks removes
 * the process from the view.
 * 
 * @author jop
 */
public class GroupException extends IOException {
	GroupException(String message, Throwable cause) {
		super(message, cause);
	}

	private static final long serialVersionUID = 1L;
}
