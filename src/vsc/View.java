package vsc;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class View implements Runnable {
	ZooKeeper zk;
	private String path;
	private String me;

	private int vid;
	private ProcesseMap active;
	private ProcesseMap leaving, oldleaving;
	private ProcesseMap entering;
	private ProcessList members, future;
	
	private Messages messages;
	private boolean awake, blocked;
	private Receiver recv;
	
	public View(String path, String me, Receiver cb) throws KeeperException, InterruptedException, IOException {
		this.zk=new ZooKeeper("localhost", 3000, null);
		this.path=path;
		this.me=me;
		this.recv=cb;
	}
		
	private void tryLeave() throws KeeperException, InterruptedException {
		//System.out.println("leaving? "+me+" ol="+oldleaving+" l="+leaving+" a="+active+" e="+entering+" m="+members+" f="+future);
		if (!blocked &&
				future==null && (oldleaving==null || oldleaving.processSet().isEmpty()) &&
				members.isKnown() && (	
				active.processSet().size()<members.processes().size() ||
				!leaving.processSet().isEmpty() ||
				!entering.processSet().isEmpty())
			) {
			blocked=true;
			recv.block();
			System.out.println("---------------- Decided to leave --------- "+me+" "+oldleaving+" "+leaving+" "+active+" "+entering+" "+members+" "+future);
		}
	}
		
	public synchronized void blockOk() throws KeeperException, InterruptedException {
		if (!blocked)
			return;
		blocked=false;
		leaving.create(messages.getLast());
		active.remove();
			
		future = new ProcessList(path+"/"+(vid+1), this);
	}
	
	private void tryEnter() throws KeeperException, InterruptedException {
		//System.out.println("entering? "+me+" "+oldleaving+" "+leaving+" "+active+" "+entering+" "+members+" "+future);
		if (future!=null &&
			!future.isKnown() &&
			active.processSet().isEmpty() &&
			(messages==null || leaving.get()>=messages.getLast())) {
			
			System.out.println("---------------- Decided to enter --------- "+me+" "+oldleaving+" "+leaving+" "+active+" "+entering+" "+members);
			
			Set<String> prop=new HashSet<String>();
			prop.addAll(leaving.processSet());
			prop.addAll(entering.processSet());
			
			future.propose(prop);		
		}
	}

	private void tryInstall() throws Exception {
		//System.out.println("installing? "+me+" "+oldleaving+" "+leaving+" "+active+" "+entering+" "+members+" "+future);

		if (future!=null && future.isKnown() && (messages==null || leaving.get()>=messages.getLast())) {
			System.out.println("---------------- Decided to install --------- "+me+" "+oldleaving+" "+leaving+" "+active+" "+entering+" "+members+" "+future);
			
			vid ++;

			oldleaving=leaving;

			active = new ProcesseMap(path+"/"+vid+"/active", me, this);
			entering = new ProcesseMap(path+"/"+vid+"/entering", me, this);
			leaving = new ProcesseMap(path+"/"+vid+"/leaving", me, this);
			
			if (future.processes().contains(me)) {
				members = future;
				future = null;
				messages = new Messages(path+"/"+vid, me, this);
				active.create(0);
			} else {
				messages = null;
				throw new Exception("kicket out");
			}

			if (oldleaving!=null)
				oldleaving.remove();
		
			install();
		}
	}
	
	private void install() {
		System.out.println("================ VIEW "+me+" "+members);
		recv.install(vid, members.processes().toArray(new String[members.processes().size()]));
	}
	
	private void tryAck() throws KeeperException, InterruptedException {
		if (messages!=null) {
			messages.xupdate();
			if (future==null)
				active.set(messages.getLast());
			else
				leaving.set(messages.getLast());
		}
	}
	
	public void enqueue(byte[] value) {
		recv.receive(value);
	}	

	private void boot() throws KeeperException, InterruptedException {
		vid=0;
		
		zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		members = new ProcessList(path+"/"+vid, this);
		members.propose(Collections.singleton(me));
		active = new ProcesseMap(path+"/"+vid+"/active", me, this);
		leaving = new ProcesseMap(path+"/"+vid+"/leaving", me, this);
		entering = new ProcesseMap(path+"/"+vid+"/entering", me, this);
		messages = new Messages(path+"/"+vid, me, this);
		
		active.create(0);
		
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
	
	/* FIXME: This is fraught with races... */
	public synchronized void join() throws KeeperException, InterruptedException {
		findView();
		if (vid<0)
			boot();
		else {
			entering = new ProcesseMap(path+"/"+vid+"/entering", me, this);
			leaving = new ProcesseMap(path+"/"+vid+"/leaving", me, this);
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