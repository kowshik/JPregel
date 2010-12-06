/**
 * 
 */
package system;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

import system.Worker.WorkerState;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class Communicator implements Runnable {

	// A map of ID to MessageSpooler
	// Or rather, a map of ID to a remote worker manager
	private Map<String, Pair<MessageSpooler, List<Message>>> idSpoolerMap;

	private List<Worker> registeredWorkers;
	private Queue<Message> msgQueue;
	private WorkerManagerImpl wkrMgr;
	private DataLocator aDataLocator;

	public DataLocator getDataLocator() {
		return aDataLocator;
	}

	public void setDataLocator(DataLocator aDataLocator) {
		this.aDataLocator = aDataLocator;
	}

	private Thread t;

	public static enum CommunicatorState {
		EXECUTE, STOP
	};

	private CommunicatorState state;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "communicator_";
	private static final String LOG_FILE_SUFFIX = ".log";
	private Logger logger;

	private String id;

	private void setId(String id) {
		this.id = id;

	}

	public String getId() {
		return id;
	}

	public synchronized void queueMessage(Message msg) {
		this.msgQueue.add(msg);
	}

	public synchronized CommunicatorState getState() {
		return state;
	}

	public synchronized void setState(CommunicatorState state) {
		this.state = state;
	}

	public Communicator(WorkerManagerImpl wkrMgr) throws IOException {
		this.setId(InetAddress.getLocalHost().getHostName() + "_"
				+ getRandomChars());
		this.initLogger();
		this.wkrMgr = wkrMgr;
		this.registeredWorkers = new Vector<Worker>();
		this.msgQueue = new LinkedList<Message>();
		this.idSpoolerMap = new HashMap<String, Pair<MessageSpooler, List<Message>>>();
		t = new Thread(this, "Communicator");
		t.start();
	}

	@Override
	public void run() {
		while (true) {
			if (this.getState() == CommunicatorState.EXECUTE) {
				
				boolean allDone = true;
				for (int index=0;index<registeredWorkers.size();index++) {
					Worker aWkr=registeredWorkers.get(index);
					if (aWkr.getState() != Worker.WorkerState.DONE) {
						allDone = false;
					}
				}
				if (allDone) {
					try {
						logger.info("Commencing communications");
						communicate();
					} catch (RemoteException e) {
						logger.severe("RemoteException occured in communicate()");
					} catch (DataNotFoundException e) {
						logger.severe("DataNotFoundException occured in communicate()");
						e.printStackTrace();
					} catch (IOException e) {
						logger.severe("IOException occured in communicate()");
						e.printStackTrace();
					} catch (NotBoundException e) {
						logger.severe("NotBoundException occured in communicate()");
						e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * @throws DataNotFoundException
	 * @throws IOException
	 * @throws NotBoundException
	 * 
	 */
	private void communicate() throws IOException, DataNotFoundException,
			NotBoundException, RemoteException {

		clearSpoolerQueues();
		populateSpoolerQueues();
		sendMessages();
		stopWorkers();
		logger.info("Ending superstep");
		this.wkrMgr.endSuperStep();
		this.setState(CommunicatorState.STOP);
	}

	/**
	 * @throws RemoteException
	 * 
	 */
	private void sendMessages() throws RemoteException {
		for (Map.Entry<String, Pair<MessageSpooler, List<Message>>> e : this.idSpoolerMap
				.entrySet()) {
			Pair<MessageSpooler, List<Message>> spoolerInfo = e.getValue();
			MessageSpooler msgSpooler = spoolerInfo.getFirst();
			List<Message> msgsToBeSent = spoolerInfo.getSecond();
			for (Message msg : msgsToBeSent) {
				logger.info("Queueing msg to spooler : " + e.getKey() + " -> "
						+ msg);
				msgSpooler.queueMessage(msg);
			}
		}

	}

	private void populateSpoolerQueues() throws IOException,
			DataNotFoundException, NotBoundException, MalformedURLException,
			RemoteException {
		Message msg;
		Map<Integer, Pair<String, String>> partitionMap = this.aDataLocator
				.readPartitionMap();
		while (!msgQueue.isEmpty()) {
			msg = msgQueue.remove();
			int targetVertexID = msg.getDestVertexID();
			int targetPartition = this.aDataLocator
					.getPartitionNumber(targetVertexID);
			Pair<String, String> targetWkrMgrInfo = partitionMap
					.get(targetPartition);
			String targetWkrMgrServiceName = targetWkrMgrInfo.getFirst();
			String targetWkrMgrHostName = targetWkrMgrInfo.getSecond();

			MessageSpooler targetMsgSpooler = null;
			List<Message> msgList = null;
			if (!this.idSpoolerMap.containsKey(targetWkrMgrServiceName)) {
				msgList = new Vector<Message>();
				if (!isSelfLookup(targetWkrMgrServiceName)) {
					logger.info("Looking up spooler service : "
							+ targetWkrMgrServiceName);

					targetMsgSpooler = (MessageSpooler) Naming.lookup("//"
							+ targetWkrMgrHostName + "/"
							+ targetWkrMgrServiceName);
				}else{
					logger.info("Avoiding self lookup for service : "
							+ targetWkrMgrServiceName);
					targetMsgSpooler=wkrMgr;
				}
				Pair<MessageSpooler, List<Message>> spoolerInfo = new Pair<MessageSpooler, List<Message>>(
						targetMsgSpooler, msgList);

				this.idSpoolerMap.put(targetWkrMgrServiceName, spoolerInfo);
				logger.info("Added spooler service and queue to map : "
						+ spoolerInfo);

			}else{
				msgList=this.idSpoolerMap.get(targetWkrMgrServiceName).getSecond();
			}
			msgList.add(msg);
			logger.info("Added message to spooler queue : "
					+ targetWkrMgrServiceName + " : " + msg);
		}
	}

	/**
	 * @return
	 */
	private boolean isSelfLookup(String targetServiceName) {
		logger.info("Checking if "+wkrMgr.getId()+" == "+targetServiceName);
		if(wkrMgr.getId().equals(targetServiceName)){
			return true;
		}
		return false;
	}

	/**
	 * 
	 */
	private void clearSpoolerQueues() {
		for (Map.Entry<String, Pair<MessageSpooler, List<Message>>> e : this.idSpoolerMap
				.entrySet()) {
			Pair<MessageSpooler, List<Message>> aPair = e.getValue();
			List<Message> thisMsgQueue = aPair.getSecond();
			thisMsgQueue.clear();			
		}
	}

	private void stopWorkers() {
		for (int index=0;index<registeredWorkers.size();index++) {
			Worker aWorker=registeredWorkers.get(index);
			aWorker.setState(WorkerState.STOP);
			logger.info("Stopped worker : " + aWorker.getId());
		}
		
	}

	public void registerWorker(Worker aWorker) {
		this.registeredWorkers.add(aWorker);
	}

	private void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(this.getId(), LOG_FILE_PREFIX
				+ this.getId() + LOG_FILE_SUFFIX);

	}

	/**
	 * 
	 * @return A random name made up of exactly three alphabets
	 */
	private static String getRandomChars() {
		char first = (char) ((new Random().nextInt(26)) + 65);
		char second = (char) ((new Random().nextInt(26)) + 65);
		char third = (char) ((new Random().nextInt(26)) + 65);
		return "" + first + second + third;
	}
}
