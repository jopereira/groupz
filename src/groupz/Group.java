package groupz;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class Group implements Runnable {
	ZooKeeper zk;
	private static final String root="/vsc";
	private String path;
	private String me;

	private int vid;
	private ProcesseMap active;
	private ProcesseMap blocked, oldblocked;
	private ProcesseMap entering;
	private ProcessList members, future;
	
	private Messages messages;
	private boolean awake, blocking;
	private Receiver recv;
	
	public Group(String gid, Receiver cb) throws KeeperException, InterruptedException, IOException {
		this.zk=new ZooKeeper("localhost", 3000, null);
		this.path=root+"/group/"+gid;
		this.recv=cb;
	}
		
	private void tryLeave() throws KeeperException, InterruptedException {
		//System.out.println("blocked? "+me+" ol="+oldblocked+" l="+blocked+" a="+active+" e="+entering+" m="+members+" f="+future);
		if (!blocking &&
				future==null && (oldblocked==null || oldblocked.processSet().isEmpty()) &&
				members.isKnown() && (	
				active.processSet().size()<members.processes().size() ||
				!blocked.processSet().isEmpty() ||
				!entering.processSet().isEmpty())
			) {
			blocking=true;
			recv.block();
			System.out.println("---------------- Decided to leave --------- "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
		}
	}
		
	public synchronized void blockOk() throws KeeperException, InterruptedException {
		if (!blocking)
			return;
		blocking=false;
		blocked.create(messages.getLast());
		active.remove();
			
		future = new ProcessList(path+"/"+(vid+1), this);
	}
	
	private void tryEnter() throws KeeperException, InterruptedException {
		//System.out.println("entering? "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
		if (future!=null &&
			!future.isKnown() &&
			active.processSet().isEmpty() &&
			(messages==null || getStability()>=messages.getLast())) {
			
			System.out.println("---------------- Decided to enter --------- "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members);
			
			Set<String> prop=new HashSet<String>();
			prop.addAll(blocked.processSet());
			prop.addAll(entering.processSet());
			
			future.propose(prop);		
		}
	}

	private void tryInstall() throws Exception {
		//System.out.println("installing? "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);

		if (future!=null && future.isKnown() && (messages==null || blocked.get()>=messages.getLast())) {
			System.out.println("---------------- Decided to install --------- "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
			
			vid ++;

			oldblocked=blocked;

			active = new ProcesseMap(path+"/"+vid+"/active", me, this);
			entering = new ProcesseMap(path+"/"+vid+"/entering", me, this);
			blocked = new ProcesseMap(path+"/"+vid+"/blocked", me, this);
			
			if (future.processes().contains(me)) {
				members = future;
				future = null;
				messages = new Messages(path+"/"+vid, me, this);
				active.create(-1);
			} else {
				messages = null;
				throw new Exception("kicket out");
			}

			if (oldblocked!=null)
				oldblocked.remove();
		
			install();
		}
	}
	
	private void install() {
		System.out.println("================ VIEW "+me+" "+members);
		recv.install(vid, members.processes().toArray(new String[members.processes().size()]));
	}
	
	private int getStability() throws KeeperException, InterruptedException {
		int lowa=active.get();
		int lowb=blocked.get();
		return lowa<lowb?lowa:lowb;
	}
	
	private void tryAck() throws KeeperException, InterruptedException {
		if (messages!=null) {
			messages.update(getStability());
			if (future==null)
				active.set(messages.getLast());
			else
				blocked.set(messages.getLast());
		}
	}
	
	public void enqueue(byte[] value) {
		recv.receive(value);
	}	

	private void createPath(String path) throws KeeperException, InterruptedException {
		try {
			zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch(KeeperException.NodeExistsException e) {
			// already exists
		}
	}
	
	private void boot() throws KeeperException, InterruptedException {
		vid=0;

		createPath(root);
		createPath(root+"/group");
		createPath(root+"/process");
		createPath(path);

		findPid();

		members = new ProcessList(path+"/"+vid, this);
		members.propose(Collections.singleton(me));
		active = new ProcesseMap(path+"/"+vid+"/active", me, this);
		blocked = new ProcesseMap(path+"/"+vid+"/blocked", me, this);
		entering = new ProcesseMap(path+"/"+vid+"/entering", me, this);
		messages = new Messages(path+"/"+vid, me, this);
		
		active.create(-1);
		
		install();
	}
	
	private void findView() {
		vid=-1;
		try {
			for(String svid: zk.getChildren(path, false)) {
				int pvid=Integer.parseInt(svid);
				if (pvid>vid)
					vid=pvid;
			}
		} catch (Exception e) {
			// not ready
		}
	}

	private void findPid() throws KeeperException, InterruptedException {
		String[] path = zk.create(root+"/process/", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL).split("/");
		me = path[path.length-1];
		System.out.println(">>>>>>>> I AM "+me);
	}
	
	/* FIXME: This is fraught with races... */
	public synchronized void join() throws KeeperException, InterruptedException {
		findView();
		if (vid<0)
			boot();
		else {
			findPid();

			entering = new ProcesseMap(path+"/"+vid+"/entering", me, this);
			blocked = new ProcesseMap(path+"/"+vid+"/blocked", me, this);
			active = new ProcesseMap(path+"/"+vid+"/active", me, this);
			future = new ProcessList(path+"/"+(vid+1), this);
			entering.create(-1);
		}
		new Thread(this).start();
		while(members==null || !members.isKnown())
			wait();
	}
	
	public synchronized void leave() throws InterruptedException, KeeperException {
		zk.close();
	}
	
	public synchronized void send(byte[] data) throws Exception {
		if (future!=null)
			throw new Exception("sending while blocked");
		messages.send(data);
	}
	
	public synchronized void run() {
		try {
			while(true) {
				while(!awake)
					wait();
				awake=false;
				tryAck();
				tryLeave();
				tryInstall();
				tryEnter();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized void wakeup() {
		awake=true;
		notifyAll();
	}
}