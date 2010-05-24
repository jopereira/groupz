/*
   Copyright 2010 José Orlando Pereira <jop@di.uminho.pt>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package groupz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
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
	static Logger logger = Logger.getLogger(Endpoint.class);
	
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
	private boolean awake;
	private Application recv;
	
	private enum State { CONNECTED, JOINED, BLOCKING, BLOCKED, DISCONNECTED };
	private State state;
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
			logger.info("created endpoint on group "+gid);
		} catch(Exception e) {
			GroupException ge=new GroupException("cannot connect to ZooKeeper", e);
			cleanup(ge);
			throw ge;
		}
	}

	/* -- Main VSC state-machine */
	
	// Pre-condition for start changing a view
	private boolean readyToBlock() throws KeeperException, InterruptedException {
		return state==State.JOINED &&
			(oldblocked==null || oldblocked.processSet().isEmpty()) &&
			(active.processSet().size()<members.processes().size() || !entering.processSet().isEmpty());
	}
		
	// Output action to start changing a view
	private void block() throws KeeperException, InterruptedException, GroupException {
		synchronized (this) {
			if (!readyToBlock()) return;
			
			state = State.BLOCKING;

			logger.info("leaving view "+vid);
		}
		
		// Callback out of synchronized!
		recv.block();
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
			blocked.create(messages.getLast());
			active.remove();
				
			future = new ProcessList(path+"/"+(vid+1), this);

			logger.info("blocked on view "+vid);
		} catch(KeeperException e) {
			onExit(e);
		} catch (InterruptedException e) {
			onExit(e);
		}
	}

	// Pre-condition for installing a new view
	private boolean readyToInstall() throws KeeperException, InterruptedException {
		return state==State.BLOCKED && active.processSet().isEmpty() &&
			(messages==null || getLastStableMessage()>=messages.getLast());
	}
	
	// Output action for installing a view
	private void install() throws KeeperException, InterruptedException, GroupException {
		String[] names=null; 

		synchronized (this) {
			if (!readyToInstall()) return;
						
			List<String> prop=new ArrayList<String>();
			// Respect order in previous view
			for(String s: members.processes())
				if (blocked.processSet().contains(s))
					prop.add(s);
			// Arriving processes in any order
			prop.addAll(entering.processSet());
			
			future.propose(prop);		
			
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
		
			logger.info("installing view "+vid);
		}
		
		// Call install out of synchronized
		recv.install(vid, names);
	}
		
	/* -- Joining and leaving a group */
	
	private void createPath(String path) throws KeeperException, InterruptedException {
		try {
			zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch(KeeperException.NodeExistsException e) {
			// already exists, don't care
		}
	}
	
	private void findPid() throws KeeperException, InterruptedException {
		String[] path = zk.create(root+"/process/", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL).split("/");
		me = path[path.length-1];
		logger.info("my process id is "+me);
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

		state=State.JOINED;

		recv.install(vid, getCurrentView());

		logger.info("new group created");
	}
	
	private int findView() {
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
		return vid;
	}

	/**
	 * Join the group. This blocks the calling thread until an initial view is
	 * installed.
	 * 
	 * @throws GroupException if the end-point is not freshly created.
	 */
	public synchronized void join() throws GroupException {
		onEntry(State.CONNECTED);
				
		try {
			int targetvid=findView();
			if (vid<0)
				boot();
			else {
				findPid();

				members = new ProcessList(path+"/"+vid, this);
				entering = new ProcessMap(path+"/"+vid+"/entering", me, this);
				blocked = new ProcessMap(path+"/"+vid+"/blocked", me, this);
				active = new ProcessMap(path+"/"+vid+"/active", me, this);
				future = new ProcessList(path+"/"+(vid+1), this);
				entering.create(-1);
				
				state=State.BLOCKED;
				
				logger.info("joining existing group");
			}

			new Thread(new Runnable() {
				public void run() {
					loop();
				}
			}).start();
			while(vid<=targetvid && state!=State.DISCONNECTED)
				wait();
		} catch(Exception e) {
			onExit(e);
		}

		if (state==State.DISCONNECTED)
			throw new GroupException("failed to join", cause);
	}

	/**
	 * Leave the group. The end-point cannot be used again.
	 */
	public synchronized void leave() {
		cleanup(null);
	}

	/* -- Message handling */
	
	private int getLastStableMessage() throws KeeperException, InterruptedException {
		int lowa=active.get();
		int lowb=blocked.get();
		return lowa<lowb?lowa:lowb;
	}

	// Pre-condition for delivering messages
	private boolean readyToDeliver() {
		return (state==State.JOINED || state==State.BLOCKING || state==State.BLOCKED) &&
			messages!=null;
	}
	
	// Action for delivering messages
	private void deliver() throws KeeperException, InterruptedException, GroupException {
		List<byte[]> values;
		
		synchronized (this) {
			if (!readyToDeliver()) return;
			
			values=messages.update(getLastStableMessage());
		}

		if (values.size()>0)
			logger.info("delivering "+values.size()+" messages");

		for(byte[] value: values)
			recv.receive(value);
		
		synchronized (this) {
			if (future==null)
				active.set(messages.getLast());
			else
				blocked.set(messages.getLast());
		}
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
	
	/* -- The rest of the public API -- */
	
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
		try {
			return members.processes().toArray(new String[members.processes().size()]);
		} catch(Exception e) {
			onExit(e);
			return null; // never happens, onExit always throws
		}
	}

	/* -- Error handling -- */
	
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
		GroupException e=new GroupException("the group is "+state+", should be "+rl, cause);
		cleanup(e);
		throw e;
	}

	private void onExit(Exception e) throws GroupException  {
		cleanup(e);
		throw new GroupException("disconnected on internal error", e);
	}

	private synchronized void cleanup(Exception cause) {
		if (state==State.DISCONNECTED)
			return;
		this.cause=cause;
		state=State.DISCONNECTED;
		if (cause!=null)
			logger.error("detached from group on error", cause);
		else
			logger.info("detached from group on leave");
		try {
			zk.close();
		} catch (InterruptedException e) {
			// don't care
		}
		wakeup();
	}
		
	/* -- State-machine main loop -- */
	
	private void loop() {
		try {
			while(true) {
				synchronized (this) {
					while(!awake && state!=State.DISCONNECTED)
						wait();
					if (state==State.DISCONNECTED)
						break;
					awake=false;
				}
				
				// The following order shouldn't matter for correctness
				deliver();
				block();
				install();
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