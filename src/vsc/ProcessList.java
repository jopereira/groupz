package vsc;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

public class ProcessList implements Watcher {
	private View view;
	private String path;
	
	private List<String> data;
	
	public ProcessList(String path, View view) throws KeeperException, InterruptedException {
		this.view=view;
		this.path=path;
		
		if (view.zk.exists(path, this)!=null)
			update();
	}

	public void propose(Collection<String> mine) throws KeeperException, InterruptedException {
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
		if (data!=null)
			return;
		try {
			byte[] value=view.zk.getData(path, this, null);
			String[] procs = new String(value).split(",");
			data = Arrays.asList(procs);
			changed();
		} catch (KeeperException.NoNodeException e) {
			// not yet
		}
	}
	
	private void changed() {
		view.wakeup();
	}

	public List<String> processes() {
		return data;
	}
	
	public String toString() {
		return "["+path+": "+data+"]";
	}

	public boolean isKnown() {
		return data!=null;
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			update();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}