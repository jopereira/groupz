package vsc;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

public class ProcesseMap implements Watcher {
	protected View view;
	private String path;
	
	private Map<String,Integer> data;
	private String me;

	public ProcesseMap(String path, String me, View view) throws KeeperException, InterruptedException {
		this.view=view;
		this.path=path;
		this.me=me;

		try {
			view.zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch(KeeperException.NodeExistsException e) {
			
		}
		update();
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			update();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void update() throws KeeperException, InterruptedException {
		Map<String,Integer> newdata=new HashMap<String, Integer>();
		for(String child: view.zk.getChildren(path, this)) {
			try {
				byte[] value=view.zk.getData(path+"/"+child, this, null);
				newdata.put(child, Integer.parseInt(new String(value)));
			} catch (KeeperException.NoNodeException e) {
				// Ignore this one...
			}
		}
		if (data==null || !data.equals(newdata)) {
			data=newdata;
			changed();
		}
	}
	
	protected void changed() {
		view.wakeup();
	}
	
	public void create(int value) throws KeeperException, InterruptedException {
		view.zk.create(path+"/"+me, Integer.toString(value).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		data.put(me, value);
	}
	
	public void set(int value) throws KeeperException, InterruptedException {
		update();
		if (value<=data.get(me))
			return;
		view.zk.setData(path+"/"+me, Integer.toString(value).getBytes(), -1);		
		data.put(me, value);
	}

	public void remove() throws InterruptedException, KeeperException {
		try {
			view.zk.delete(path+"/"+me, -1);
			data.remove(me);
		} catch(KeeperException.NoNodeException e) {
			// don't care
		}
	}
	
	public int get() throws KeeperException, InterruptedException {
		update();
		int min=Integer.MAX_VALUE;
		for(int i: data.values()) {
			if (i<min)
				min=i;
		}
		return min;
	}
	
	public Set<String> processSet() throws KeeperException, InterruptedException {
		update();
		return data.keySet();
	}
	
	public String toString() {
		return "["+path+": "+data+"]";
	}
}
