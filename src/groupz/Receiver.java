package groupz;

public interface Receiver {
	public void receive(byte[] data);
	public void install(int vid, String[] members);
	public void block();
}
