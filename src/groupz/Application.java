
package groupz;

/**
 * Application callbacks for group communication. The application must
 * implement this interface to handle group events. Any exceptions thrown
 * within application code will remove the process from the group.
 * 
 * @author jop
 */
public interface Application {
	/**
	 * Handle a message.
	 * @param data raw message data, as sent by a process
	 * @throws GroupException an exception that might occur while trying to
	 * perform other group operations
	 */
	public void receive(byte[] data) throws GroupException;
	
	/**
	 * Handle a new view.
	 * @param vid a monotonically increasing view indentifier
	 * @param members the current members of the group, or null if the process
	 * has been excluded
	 * @throws GroupException an exception that might occur while trying to
	 * perform other group operations
	 */
	public void install(int vid, String[] members) throws GroupException;
	
	/**
	 * Prepare for a new view. A process should stop sending messages and
	 * then call blockOk() to allow the installation of a new view to proceed.
	 * @throws GroupException an exception that might occur while trying to
	 * perform other group operations
	 */
	public void block() throws GroupException;
}
