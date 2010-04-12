package groupz;

public class GroupException extends Exception {
	public GroupException(String message) {
		super(message);
	}

	public GroupException(String message, Throwable cause) {
		super(message, cause);
	}

	private static final long serialVersionUID = 1L;
}
