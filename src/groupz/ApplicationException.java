package groupz;

/**
 * Wrapped application exception. An application exception during a group
 * call back (e.g. install or receive) can be wrapped with this class and
 * re-thrown. This will make the process leave the group, as it usually
 * signals a bug or otherwise corrupted application state. 
 * 
 * @author jop
 */
public class ApplicationException extends GroupException {
	/**
	 * Wrap an exception.
	 * 
	 * @param cause the wrapped exception
	 */
	public ApplicationException(Throwable cause) {
		super("unrecoverable application exception", cause);
	}
}
