/**
 * 
 */
package system;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
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

import exceptions.DataNotFoundException;

import system.Worker.WorkerState;
import utility.JPregelLogger;
import utility.Pair;

/**
 * This class aggregates messages which are meant to be other machines at the
 * end of a superstep. This reduces the number of RMI class destined to any
 * machine in the cluster.
 * 
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

	/**
	 * 
	 * @return DataLocator DataLocator internally stored in this class
	 */
	public DataLocator getDataLocator() {
		return aDataLocator;
	}

	/**
	 * Sets the DataLocator internally stored in this class
	 * 
	 * @param aDataLocator
	 */

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

	/**
	 * Gets the class ID
	 * 
	 * @return
	 */
	public String getId() {
		return id;
	}

	/**
	 * Queues an incoming message from a Worker thread
	 * 
	 * @param msg
	 */
	public synchronized void queueMessage(Message msg) {
		this.msgQueue.add(msg);
	}

	/**
	 * Returns the state of the Communicator
	 * 
	 * @return
	 */
	public synchronized CommunicatorState getState() {
		return state;
	}

	/**
	 * Sets the state of the Communicator
	 * 
	 * @param state
	 */
	public synchronized void setState(CommunicatorState state) {
		this.state = state;
	}

	/**
	 * 
	 * @param wkrMgr
	 *            WorkerManager that owns this Communicator
	 * @param ID
	 *            of the owner
	 * @throws IOException
	 */

	public Communicator(WorkerManagerImpl wkrMgr, String id) throws IOException {
		this.setId(id + "_" + this.getRandomChars());
		this.initLogger();
		this.wkrMgr = wkrMgr;
		this.registeredWorkers = new Vector<Worker>();
		this.msgQueue = new LinkedList<Message>();
		this.idSpoolerMap = new HashMap<String, Pair<MessageSpooler, List<Message>>>();
		t = new Thread(this, "Communicator");
		t.start();
	}

	// The communicator loops forever polling workers to check if the have
	// transitioned states

	@Override
	public void run() {
		while (true) {
			if (this.getState() == CommunicatorState.EXECUTE) {

				boolean allDone = true;
				for (int index = 0; index < registeredWorkers.size(); index++) {
					Worker aWkr = registeredWorkers.get(index);
					if (aWkr.getState() != Worker.WorkerState.DONE) {
						allDone = false;
					}
				}
				if (allDone) {
					try {
						logger.info("Commencing communications");
						communicate();
					} catch (UnknownHostException e) {
						logger.severe("UnknownHostException occured in communicate()");
						e.printStackTrace();
					} catch (RemoteException e) {
						logger.severe("RemoteException occured in communicate()");
						logger.severe(e.toString());
						e.printStackTrace();
					} catch (DataNotFoundException e) {
						logger.severe("DataNotFoundException occured in communicate()");
						e.printStackTrace();
					} catch (IOException e) {
						logger.severe("IOException occured in communicate()");
						e.printStackTrace();
					} catch (NotBoundException e) {
						logger.severe("NotBoundException occured in communicate()");
						e.printStackTrace();
					} finally {
						logger.info("Setting communicator state to STOP");
						this.setState(CommunicatorState.STOP);
					}
				}
			}
		}
	}

	/**
	 * All the communication happens here 
	 * @throws DataNotFoundException
	 * @throws IOException
	 * @throws NotBoundException
	 * 
	 */
	private void communicate() throws IOException, DataNotFoundException,
			NotBoundException, RemoteException, UnknownHostException {

		clearSpoolerQueues();
		populateSpoolerQueues();
		sendMessages();
		stopWorkers();
		logger.info("Ending superstep");
		this.wkrMgr.endSuperStep();
		this.setState(CommunicatorState.STOP);
	}

	/**
	 * 
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

	//Populates message spooler queues for all machines
	private void populateSpoolerQueues() throws IOException,
			DataNotFoundException, NotBoundException, MalformedURLException,
			RemoteException, UnknownHostException {
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
			String targetWkrMgrHostName = targetWkrMgrInfo.getSecond().split(
					":")[0];
			String targetHostPort = targetWkrMgrInfo.getSecond().split(":")[1];

			MessageSpooler targetMsgSpooler = null;
			List<Message> msgList = null;
			if (!this.idSpoolerMap.containsKey(targetWkrMgrServiceName)) {
				msgList = new Vector<Message>();
				if (isSelfLookup(targetWkrMgrServiceName)) {

					logger.info("Avoiding self lookup for service : "
							+ targetWkrMgrServiceName);
					targetMsgSpooler = wkrMgr;

				} else if (isSameHost(targetWkrMgrHostName)) {
					logger.info("In current host : " + targetWkrMgrHostName
							+ " looking up spooler service : " + "localhost:"
							+ targetHostPort + "/" + targetWkrMgrServiceName);

					targetMsgSpooler = (MessageSpooler) Naming.lookup("//"
							+ "localhost:" + targetHostPort + "/"
							+ targetWkrMgrServiceName);
				}

				else {
					logger.info("Looking up spooler service : "
							+ targetWkrMgrHostName + ":" + targetHostPort + "/"
							+ targetWkrMgrServiceName);

					targetMsgSpooler = (MessageSpooler) Naming.lookup("//"
							+ targetWkrMgrHostName + ":" + targetHostPort + "/"
							+ targetWkrMgrServiceName);
				}
				Pair<MessageSpooler, List<Message>> spoolerInfo = new Pair<MessageSpooler, List<Message>>(
						targetMsgSpooler, msgList);

				this.idSpoolerMap.put(targetWkrMgrServiceName, spoolerInfo);
				logger.info("Added spooler service and queue to map : "
						+ spoolerInfo);

			} else {
				msgList = this.idSpoolerMap.get(targetWkrMgrServiceName)
						.getSecond();
			}
			msgList.add(msg);
			logger.info("Added message to spooler queue : "
					+ targetWkrMgrServiceName + " : " + msg);
		}
	}

	/**
	 * 
	 * @param targetWkrMgrHostName
	 * @return
	 * @throws
	 */
	private boolean isSameHost(String targetWkrMgrHostName)
			throws UnknownHostException {
		String hostName = InetAddress.getLocalHost().getHostName();
		return targetWkrMgrHostName.equals(hostName);
	}

	/**
	 * @return
	 */
	private boolean isSelfLookup(String targetServiceName) {
		logger.info("Checking if " + wkrMgr.getId() + " == "
				+ targetServiceName);
		if (wkrMgr.getId().equals(targetServiceName)) {
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
		for (int index = 0; index < registeredWorkers.size(); index++) {
			Worker aWorker = registeredWorkers.get(index);
			aWorker.setState(WorkerState.STOP);
			logger.info("Stopped worker : " + aWorker.getId());
		}

	}

	/**
	 * 
	 * @param aWorker A Worker thread that should be polled for state changes
	 */
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
