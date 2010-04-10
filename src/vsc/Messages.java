package vsc;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

public class Messages implements Watcher {
	private View view;
	private String path;
	
	private int last=-1;
	
	public Messages(String path, String me, View view) throws KeeperException, InterruptedException {
		this.view=view;
		this.path=path+"/messages";

		create();
		update();
	}

	private void create() throws KeeperException, InterruptedException {
		try {
			view.zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			// already done
		}
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			update();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void update() throws NumberFormatException, KeeperException, InterruptedException {
		int before=last;
		for(String child: view.zk.getChildren(path, this)) {
			int id=Integer.parseInt(child.substring(1));
			if (id>last) {
				byte[] value=view.zk.getData(path+"/"+child, null, null);
				view.enqueue(value);
				last++;
			}
		}
		if (last>before)
			changed();
	}
	
	private void changed() {
		view.wakeup();
	}
	
	public void send(byte[] data) throws KeeperException, InterruptedException {
		view.zk.create(path+"/m", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
	}
	
	public int getLast() {
		return last;
	}
}
