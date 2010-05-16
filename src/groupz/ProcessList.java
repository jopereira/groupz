package groupz;

import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

class ProcessList implements Watcher {
	private Endpoint view;
	private String path;
	
	private List<String> data;
	
	public ProcessList(String path, Endpoint view) throws KeeperException, InterruptedException {
		this.view=view;
		this.path=path;
		
		if (view.zk.exists(path, this)!=null)
			update();
	}

	public void propose(List<String> mine) throws KeeperException, InterruptedException {
		String value=null;
		for(String v: mine)
			if (value==null)
				value=v;
			else
				value+=","+v;
		try {
			view.zk.create(path, value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			// not mine...
		}
		update();
	}

	private void update() throws KeeperException, InterruptedException {
		synchronized (this) {
			try {
				byte[] value=view.zk.getData(path, this, null);
				String[] procs = new String(value).split(",");
				data = Arrays.asList(procs);
			} catch (KeeperException.NoNodeException e) {
				// not yet
			}
		}
	}
	
	public synchronized List<String> processes() throws KeeperException, InterruptedException {
		if (data==null)
			update();
		return data;
	}
	
	public synchronized boolean isKnown() throws KeeperException, InterruptedException {
		if (data==null)
			update();
		return data!=null;
	}

	@Override
	public void process(WatchedEvent event) {
		view.wakeup();
	}

	public String toString() {
		return "["+path+": "+data+"]";
	}
}
