package groupz;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class Group {
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
	
	private enum State { UNINITIALIZED, CONNECTED, JOINED, BLOCKING, BLOCKED, DISCONNECTED };
	private State state = State.UNINITIALIZED;
	
	public Group(String gid, Receiver cb) throws GroupException {
		try {
			this.zk=new ZooKeeper("localhost", 3000, null);
			this.path=root+"/group/"+gid;
			this.recv=cb;
			this.state=State.CONNECTED;
		} catch(Exception e) {
			cleanup();
			throw new GroupException("cannot connect to ZooKeeper", e);
		}
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
			state = State.BLOCKING;
			System.out.println("---------------- Decided to leave --------- "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
			try {
				recv.block();
			} catch(GroupException e) {
				// let it fall through
			}
		}
	}
		
	public synchronized void blockOk() throws GroupException {
		onEntry(State.BLOCKING);

		try {
			state = State.BLOCKED;
			blocking=false;
			blocked.create(messages.getLast());
			active.remove();
				
			future = new ProcessList(path+"/"+(vid+1), this);
		} catch(KeeperException e) {
			onExit(e);
		} catch (InterruptedException e) {
			onExit(e);
		}
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
				state = State.JOINED;
			} else {
				messages = null;
				cleanup();
			}

			if (oldblocked!=null)
				oldblocked.remove();
		
			install();
		}
	}
	
	private void install() {
		System.out.println("================ VIEW "+me+" "+members);
		try {
			if (state==State.JOINED)
				recv.install(vid, members.processes().toArray(new String[members.processes().size()]));
			else
				recv.install(vid, null);
		} catch(GroupException e) {
			// let it fall through
		}
	}
	
	private int getStability() throws KeeperException, InterruptedException {
		int lowa=active.get();
		int lowb=blocked.get();
		return lowa<lowb?lowa:lowb;
	}
	
	private void tryAck() throws KeeperException, InterruptedException {
		if (messages!=null) {
			List<byte[]> values=messages.update(getStability());
			try {
				for(byte[] value: values)
					recv.receive(value);
				if (future==null)
					active.set(messages.getLast());
				else
					blocked.set(messages.getLast());
			} catch(GroupException e) {
				// let it fall through
			}
		}
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
	
	private synchronized void cleanup() {
		if (state==State.DISCONNECTED)
			return;
		state=State.DISCONNECTED;
		try {
			zk.close();
		} catch (InterruptedException e) {
			// don't care
		}
		wakeup();
	}
	
	private void onEntry(State... reqs) throws GroupException  {
		for(State req: reqs)
			if (req==state)
				return;
		String rl=null;
		for(State req: reqs)
			if (rl==null)
				rl=req.toString();
			else
				rl+=" or "+req;
		cleanup();
		throw new GroupException("the group is "+state+", should be "+rl);
	}

	private void onExit(Exception e) throws GroupException  {
		cleanup();
		throw new GroupException("disconnected on internal error", e);
	}
	
	/* FIXME: This is fraught with races... */
	public synchronized void join() throws GroupException {
		onEntry(State.CONNECTED);
		
		try {
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
			new Thread(new Runnable() {
				public void run() {
					mainLoop();
				}
			}).start();
			while(members==null || !members.isKnown())
				wait();
		} catch(Exception e) {
			onExit(e);
		}
	}
	
	public synchronized void leave() {
		cleanup();
	}
	
	public synchronized void send(byte[] data) throws GroupException {
		onEntry(State.JOINED, State.BLOCKING);
		
		try {
			messages.send(data);
		} catch (KeeperException e) {
			onExit(e);
		} catch (InterruptedException e) {
			onExit(e);
		}
	}
	
	private synchronized void mainLoop() {
		try {
			while(true) {
				while(!awake && state!=State.DISCONNECTED)
					wait();
				if (state==State.DISCONNECTED)
					break;
				awake=false;
				tryAck();
				tryLeave();
				tryInstall();
				tryEnter();
			}
		} catch(Exception e) {
			cleanup();
		}
	}

	synchronized void wakeup() {
		awake=true;
		notifyAll();
	}
}