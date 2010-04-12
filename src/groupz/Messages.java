package groupz;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

class Messages implements Watcher {
	private Group view;
	private String path;
	
	private int last=-1;
	
	public Messages(String path, String me, Group view) throws KeeperException, InterruptedException {
		this.view=view;
		this.path=path+"/messages";

		create();
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
			view.wakeup();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized List<byte[]> update(int low) throws NumberFormatException, KeeperException, InterruptedException {
		List<byte[]> data=new ArrayList<byte[]>();
		SortedSet<String> childs=new TreeSet<String>();
		childs.addAll(view.zk.getChildren(path, this));
		for(String child: childs) {
			int id=Integer.parseInt(child);
			if (id>last) {
				byte[] value=view.zk.getData(path+"/"+child, null, null);
				data.add(value);
				last=id;
			} else if (id<=low){
				try {
					view.zk.delete(path+"/"+child, -1);
				} catch(KeeperException.NoNodeException e) {
					// someone got there first...
				}
			}
		}
		return data;
	}
	
	public void send(byte[] data) throws KeeperException, InterruptedException {
		view.zk.create(path+"/", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
	}
	
	public synchronized int getLast() {
		return last;
	}
}
