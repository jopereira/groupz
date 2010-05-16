package groupz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Group communication end-point. It provides virtually synchronous closed
 * group communication, including view synchrony and totally ordered multicast.
 *  
 * @author jop
 */
public class Endpoint {
	ZooKeeper zk;
	private static final String root="/vsc";
	private String path;
	private String me;

	private int vid;
	private ProcessMap active;
	private ProcessMap blocked, oldblocked;
	private ProcessMap entering;
	private ProcessList members, future;
	
	private Messages messages;
	private boolean awake, blocking;
	private Application recv;
	
	private enum State { UNINITIALIZED, CONNECTED, JOINED, BLOCKING, BLOCKED, DISCONNECTED };
	private State state = State.UNINITIALIZED;
	private Exception cause;

	/**
	 * Initialize a group communication end-point.
	 * 
	 * @param gid a group identifier
	 * @param cb application callbacks
	 * @throws GroupException if a local ZooKeeper server cannot be used
	 */
	public Endpoint(String gid, Application cb) throws GroupException {
		try {
			this.zk=new ZooKeeper("localhost", 3000, null);
			this.path=root+"/group/"+gid;
			this.recv=cb;
			this.state=State.CONNECTED;
		} catch(Exception e) {
			cleanup(e);
			throw new GroupException("cannot connect to ZooKeeper", e);
		}
	}
		
	private void tryLeave() throws KeeperException, InterruptedException {
		//System.out.println("blocked? "+me+" ol="+oldblocked+" l="+blocked+" a="+active+" e="+entering+" m="+members+" f="+future);
		synchronized (this) {
			if (!(!blocking &&
					future==null && (oldblocked==null || oldblocked.processSet().isEmpty()) &&
					members.isKnown() && (	
							active.processSet().size()<members.processes().size() ||
							!blocked.processSet().isEmpty() ||
							!entering.processSet().isEmpty())
				))
				return;
			
			
			blocking=true;
			state = State.BLOCKING;
			//System.out.println("---------------- Decided to leave --------- "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
		}
		// Callback out of synchronized!
		callBlock();
	}

	private void callBlock() {
		try {
			recv.block();
		} catch(GroupException e) {
			// let it fall through
		} catch(Exception e) {
			cleanup(e);
		}
	}
	
	/**
	 * Allow view change to proceed, after the block() callback has
	 * been invoked. This means that the application cannot send more
	 * messages until a new view has been installed. 
	 * 
	 * @throws GroupException if the group is not trying to block
	 */
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
	
	private synchronized void tryEnter() throws KeeperException, InterruptedException {
		//System.out.println("entering? "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
		if (future!=null &&
			!future.isKnown() &&
			active.processSet().isEmpty() &&
			(messages==null || getStability()>=messages.getLast())) {
			
			//System.out.println("---------------- Decided to enter --------- "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members);
			
			List<String> prop=new ArrayList<String>();
			prop.addAll(blocked.processSet());
			prop.addAll(entering.processSet());
			
			future.propose(prop);		
		}
	}

	private synchronized void tryInstall() throws Exception {
		//System.out.println("installing? "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
		
		String[] names=null; 
		
		synchronized (this) {
			if (!(future!=null && future.isKnown() && (messages==null || blocked.get()>=messages.getLast())))
				return;
			
			//System.out.println("---------------- Decided to install --------- "+me+" "+oldblocked+" "+blocked+" "+active+" "+entering+" "+members+" "+future);
			
			vid ++;

			oldblocked=blocked;

			active = new ProcessMap(path+"/"+vid+"/active", me, this);
			entering = new ProcessMap(path+"/"+vid+"/entering", me, this);
			blocked = new ProcessMap(path+"/"+vid+"/blocked", me, this);
			
			if (future.processes().contains(me)) {
				members = future;
				future = null;
				messages = new Messages(path+"/"+vid, me, this);
				active.create(-1);
				state = State.JOINED;
				names = getCurrentView();
			} else {
				messages = null;
				cleanup(null);
			}

			if (oldblocked!=null)
				oldblocked.remove();
		
		}
		
		// Call install out of synchronized
		callInstall(vid, names);
	}
	
	private synchronized void callInstall(int v, String[] names) {
		//System.out.println("================ VIEW "+me+" "+members);
		try {
			recv.install(v, names);
		} catch(GroupException e) {
			// let it fall through
		} catch(Exception e) {
			cleanup(e);
		}
	}
	
	private int getStability() throws KeeperException, InterruptedException {
		int lowa=active.get();
		int lowb=blocked.get();
		return lowa<lowb?lowa:lowb;
	}
	
	private void tryAck() throws KeeperException, InterruptedException {
		List<byte[]> values;
		
		synchronized (this) {
			if (state!=State.JOINED && state!=State.BLOCKING && state!=State.BLOCKED)
				return;
		
			if (messages==null)
				return;
			
			values=messages.update(getStability());
		}
		
		try {
			for(byte[] value: values)
				recv.receive(value);
		} catch(GroupException e) {
			// let it fall through
			return;
		} catch(Exception e) {
			cleanup(e);
			return;
		}
		
		synchronized (this) {
			if (future==null)
				active.set(messages.getLast());
			else
				blocked.set(messages.getLast());
		}
	}
	
	private void createPath(String path) throws KeeperException, InterruptedException {
		try {
			zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch(KeeperException.NodeExistsException e) {
			// already exists
		}
	}
	
	private void boot() throws KeeperException, InterruptedException, GroupException {
		vid=0;

		createPath(root);
		createPath(root+"/group");
		createPath(root+"/process");
		createPath(path);

		findPid();

		members = new ProcessList(path+"/"+vid, this);
		members.propose(Collections.singletonList(me));
		active = new ProcessMap(path+"/"+vid+"/active", me, this);
		blocked = new ProcessMap(path+"/"+vid+"/blocked", me, this);
		entering = new ProcessMap(path+"/"+vid+"/entering", me, this);
		messages = new Messages(path+"/"+vid, me, this);
		
		active.create(-1);
		
		callInstall(vid, getCurrentView());
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
		//System.out.println(">>>>>>>> I AM "+me);
	}
	
	private synchronized void cleanup(Exception cause) {
		if (state==State.DISCONNECTED)
			return;
		this.cause=cause;
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
		cleanup(null);
		if (cause!=null)
			throw new GroupException("the group is "+state+", should be "+rl, cause);
		else
			throw new GroupException("the group is "+state+", should be "+rl);
	}

	private void onExit(Exception e) throws GroupException  {
		cleanup(e);
		throw new GroupException("disconnected on internal error", e);
	}
	
	///* FIXME: This is fraught with races... */
	
	/**
	 * Join the group. This blocks the calling thread until an initial view is
	 * installed.
	 * 
	 * @throws GroupException if the end-point is not freshly created.
	 */
	public synchronized void join() throws GroupException {
		onEntry(State.CONNECTED);
		
		try {
			findView();
			if (vid<0)
				boot();
			else {
				findPid();
	
				entering = new ProcessMap(path+"/"+vid+"/entering", me, this);
				blocked = new ProcessMap(path+"/"+vid+"/blocked", me, this);
				active = new ProcessMap(path+"/"+vid+"/active", me, this);
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
	
	/**
	 * Leave the group. The end-point cannot be used again.
	 */
	public synchronized void leave() {
		cleanup(null);
	}
	
	/**
	 * Send a message. This cannot be invoked after blockOk() has been called
	 * until a new view is installed.
	 * 
	 * @throws GroupException if the end-point is not freshly created.
	 */
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

	/**
	 * Get a unique identifier of the local process.
	 * 
	 * @return the identification of the local process
	 * @throws GroupException if no view is installed
	 */
	public synchronized String getProcessId() throws GroupException {
		onEntry(State.JOINED, State.BLOCKING, State.BLOCKED);		
		return me;
	}
	
	/**
	 * Get the current composition of the view. This is guaranteed to 
	 * be exactly the same in all members.
	 * 
	 * @return the list of group members
	 * @throws GroupException if no view is installed
	 */
	public synchronized String[] getCurrentView() throws GroupException {
		onEntry(State.JOINED, State.BLOCKING, State.BLOCKED);
		return members.processes().toArray(new String[members.processes().size()]);		
	}
	
	private void mainLoop() {
		try {
			while(true) {
				synchronized (this) {
					while(!awake && state!=State.DISCONNECTED)
						wait();
					if (state==State.DISCONNECTED)
						break;
					awake=false;
				}
				tryAck();
				tryLeave();
				tryInstall();
				tryEnter();
			}
		} catch(Exception e) {
			cleanup(e);
		}
	}

	synchronized void wakeup() {
		awake=true;
		notifyAll();
	}
}