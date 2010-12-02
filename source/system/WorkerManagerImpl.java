/**
 * 
 */
package system;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class WorkerManagerImpl extends UnicastRemoteObject implements
		WorkerManager {

	private String id;
	private Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "workermanager_";
	private static final String LOG_FILE_SUFFIX = ".log";
	private ManagerToMaster master;

	private List<Worker> workers;

	private void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(this.getId(), LOG_FILE_PREFIX
				+ this.getId() + LOG_FILE_SUFFIX);
	}

	private WorkerManagerImpl() throws IOException {
		this.workers = new Vector<Worker>();
		this.setId(InetAddress.getLocalHost().getHostName() + "_"
				+ WorkerManagerImpl.getRandomChars());
		this.initLogger();
	}

	public WorkerManagerImpl(ManagerToMaster master) throws IOException {
		this();
		this.master = master;

	}

	/**
	 * @param string
	 */
	private void setId(String id) {
		this.id = id;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.WorkerManager#compute()
	 */
	@Override
	public void compute() throws RemoteException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.WorkerManager#getId()
	 */
	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return id;
	}

	public static void main(String[] args) throws IOException {
		String masterServer = args[0];
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		try {

			ManagerToMaster master = (ManagerToMaster) Naming.lookup("//"
					+ masterServer + "/" + ManagerToMaster.SERVICE_NAME);
			WorkerManagerImpl mgr = new WorkerManagerImpl(master);
			master.register(mgr, mgr.getId());
			System.out.println("Worker Manager ready : " + mgr.getId());
		} catch (RemoteException e) {
			System.err.println("WorkerManagerImpl Remote exception : ");
			e.printStackTrace();

		} catch (MalformedURLException e) {
			System.err.println("WorkerManagerImpl Malformed exception : ");
			e.printStackTrace();
		} catch (NotBoundException e) {
			System.err.println("WorkerManagerImpl NotBound exception : ");
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @return A random name made up of exactly three alphabets
	 */
	public static String getRandomChars() {
		char first = (char) ((new Random().nextInt(26)) + 65);
		char second = (char) ((new Random().nextInt(26)) + 65);
		char third = (char) ((new Random().nextInt(26)) + 65);
		return "" + first + second + third;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.WorkerManager#initialize(java.util.List)
	 */
	@Override
	public void initialize(List<Integer> partitionNumbers, int numWorkers)
			throws RemoteException {
		logger.info("Received partitionNumbers : " + partitionNumbers);
		Iterator<Integer> it = partitionNumbers.iterator();
		List<Integer> threadPartitions = new Vector<Integer>();
		
		int workerIndex = 0;
		int wkrPartitionCount = partitionNumbers.size() / numWorkers;
		if(wkrPartitionCount == 0 && numWorkers!=0){
			wkrPartitionCount=partitionNumbers.size();
		}
		while (it.hasNext() && (workers.size() != numWorkers)) {
			int thisWkrPartitionCount = 0;
			threadPartitions.clear();
			while (thisWkrPartitionCount < wkrPartitionCount && it.hasNext()) {
				threadPartitions.add(it.next());
				thisWkrPartitionCount++;
			}
			if (thisWkrPartitionCount > 0) {
				try {
					if (it.hasNext() && this.workers.size() + 1 == numWorkers) {
						while (it.hasNext()) {
							threadPartitions.add(it.next());
						}
					}
					Worker aWkr = new Worker(new Vector<Integer>(
							threadPartitions), this);
					this.workers.add(aWkr);
					logger.info("Added new worker : " + aWkr);
				} catch (DataNotFoundException e) {
					logger.severe("Unable to read partitions in worker manager : "
							+ this.getId());
					e.printStackTrace();
				} catch (IOException e) {
					logger.severe("Unable to read partitions in worker manager : "
							+ this.getId());
					e.printStackTrace();
				} catch (IllegalInputException e) {
					logger.severe("Unable to read partitions in worker manager : "
							+ this.getId());
					e.printStackTrace();
				}
			}

		}

		logger.info("Initialized worker manager : " + this.getId()
				+ "\n\n Workers are : " + workers);

	}

}
