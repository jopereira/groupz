package groupz;

public interface Receiver {
	public void receive(byte[] data) throws GroupException;
	public void install(int vid, String[] members) throws GroupException;
	public void block() throws GroupException;
}
