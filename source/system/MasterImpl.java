/**
 * 
 */
package system;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

import system.MasterImpl.FaultDetector;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class MasterImpl extends UnicastRemoteObject implements ManagerToMaster,
		MasterToClient, Runnable {

	/**
	 * 
	 */
	private int superStep;

	public int getSuperStep() {
		return superStep;
	}

	public void setSuperStep(int superStep) {
		this.superStep = superStep;
	}

	private static final long serialVersionUID = -7962409918852099855L;

	private Thread superstepExecutorThread;
	private Thread faultDetectorThread;
	public Map<String, WorkerManager> idManagerMap;
	private Map<WorkerManager, WORKER_MANAGER_STATE> activeWorkerMgrs;
	private Logger logger;
	private String id;
	private String vertexClassName;

	private boolean allDone;

	private int activeMgrs;

	private int numMachines;

	private FaultDetector aFaultDetector;

	public String getVertexClassName() {
		return vertexClassName;
	}

	public void setVertexClassName(String vertexClassName) {
		this.vertexClassName = vertexClassName;
		logger.info("set vertexClassName : " + vertexClassName);
	}

	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "master";
	private static final String LOG_FILE_SUFFIX = ".log";
	private static final int PORT_NUMBER = 3672;

	
	public class FaultDetector implements Runnable {
		public FaultDetector(){
			
		}
		@Override
		public void run(){
			
		}
		
		public String getID() {
			return "FaultDetector";
		}
	}
	public MasterImpl(String vertexClassName, int numMachines) throws IOException {

		this.setId("Master");
		initLogger();
		this.setNumMachines(numMachines);
		this.setSuperStep(JPregelConstants.FIRST_SUPERSTEP);
		this.setVertexClassName(vertexClassName);
		this.idManagerMap = new HashMap<String, WorkerManager>();
		this.activeWorkerMgrs = new HashMap<WorkerManager, WORKER_MANAGER_STATE>();
		this.superstepExecutorThread = new Thread(this, getId());
		this.aFaultDetector=new FaultDetector();
		this.faultDetectorThread=new Thread(this.aFaultDetector,this.aFaultDetector.getID());
		
	}

	/**
	 * @param numMachines
	 */
	private void setNumMachines(int numMachines) {
		this.numMachines=numMachines;
		
	}
	
	/**
	 * @param numMachines
	 */
	private int getNumMachines() {
		return this.numMachines;
		
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	private void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(getId(), LOG_FILE_PREFIX
				+ LOG_FILE_SUFFIX);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.ManagerToMaster#register(system.WorkerManager,
	 * java.lang.String)
	 */
	@Override
	public synchronized void register(WorkerManager aWorkerManager, String id)
			throws RemoteException {
		this.idManagerMap.put(id, aWorkerManager);
		logger.info("registered worker manager : " + id);
		logger.info("size of map : " + idManagerMap.size());
		this.activeWorkerMgrs.put(aWorkerManager, WORKER_MANAGER_STATE.ACTIVE);
		logger.info("registered state of worker : " + id + " to "
				+ WORKER_MANAGER_STATE.ACTIVE);

		if (idManagerMap.size() == this.numMachines) {
			executeTask();
		}

	}

	public static void main(String args[]) throws IllegalClassException {
		String vertexClassName = args[0];
		int numMachines= Integer.parseInt(args[1]);
		try {
			Class c = Class.forName(vertexClassName);
			if (!c.getSuperclass().equals(Vertex.class)) {
				throw new IllegalClassException(vertexClassName);
			}

		} catch (ClassNotFoundException e) {
			System.err.println("Client vertex class not found !");
			e.printStackTrace();
		}
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		try {

			MasterToClient master = new MasterImpl(vertexClassName,numMachines);
			Registry registry = LocateRegistry.createRegistry(PORT_NUMBER);
			registry.rebind(MasterToClient.SERVICE_NAME, master);
			System.err.println("Master instance bound");
		} catch (Exception e) {
			System.err.println("Can't bind Master instance");
			e.printStackTrace();

		}
	}

	public int getWorkerMgrsCount() {
		logger.info("returning : " + idManagerMap.size());
		return this.idManagerMap.size();
	}

	public int getWorkerMgrThreads() {
		return JPregelConstants.WORKER_MGR_THREADS;
	}

	public void executeTask() {

		superstepExecutorThread.start();
		faultDetectorThread.start();
	}

	public void run() {
		try {

			initializeWorkerManagers();
			while (findActiveManagers(this.getSuperStep()) > 0) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				this.setAllDone(false);

				this.activeMgrs = this.idManagerMap.size();
				for (Map.Entry<String, WorkerManager> e : this.idManagerMap
						.entrySet()) {
					String aWkrMgrId = e.getKey();
					WorkerManager aWkrMgr = e.getValue();

					logger.info("Commencing superstep : " + this.getSuperStep()
							+ " in worker manager : " + aWkrMgrId);
					aWkrMgr.beginSuperStep(this.getSuperStep(),
							this.isCheckPoint());
				}
				logger.info("Waiting for worker managers to complete execution");
				while (!allDone()) {

				}
				logger.info("Superstep over : " + this.getSuperStep());
				this.setSuperStep(this.getSuperStep() + 1);
			}
			logger.info("-----------------------------------------------------");
			logger.info("Writing Solutions");
			// Writing solutions
			writeSolutions();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalInputException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DataNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * @return
	 */
	private boolean isCheckPoint() {
		if (this.getSuperStep() % JPregelConstants.CHECKPOINT_INTERVAL == 0) {
			return true;
		}
		return false;
	}

	/**
	 * @throws RemoteException
	 * 
	 */
	private void writeSolutions() throws RemoteException {
		for (Map.Entry<String, WorkerManager> e : this.idManagerMap.entrySet()) {
			WorkerManager aWkrMgr = e.getValue();
			aWkrMgr.writeSolutions();
		}

	}

	/**
	 * @return
	 * @throws RemoteException
	 */
	private int findActiveManagers(int superStep) throws RemoteException {
		if (superStep == JPregelConstants.FIRST_SUPERSTEP) {
			return this.idManagerMap.size();
		}
		int activeManagers = 0;
		for (Map.Entry<String, WorkerManager> e : this.idManagerMap.entrySet()) {
			MessageSpooler aSpooler = (MessageSpooler) e.getValue();
			if (aSpooler.getQueueSize() > 0) {
				activeManagers++;
			}
		}
		return activeManagers;

	}

	/**
	 * @return
	 */
	private synchronized boolean allDone() {
		return allDone;
	}

	/**
	 * @throws IOException
	 * @throws DataNotFoundException
	 * @throws IllegalInputException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * 
	 */
	private void initializeWorkerManagers() throws IOException,
			IllegalInputException, DataNotFoundException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		GraphPartitioner gp = new GraphPartitioner(JPregelConstants.GRAPH_FILE,
				this, this.vertexClassName);
		int numPartitions = gp.partitionGraphs();

		int numMgrPartitions = numPartitions / this.getWorkerMgrsCount();
		List<Integer> wkrMgrPartitions = new Vector<Integer>();
		int partitionCount = 0;
		WorkerManager thisWkrMgr = null;
		String thisWkrMgrId = null;
		String thisWkrMgrName = null;
		Map<Integer, Pair<String, String>> partitionWkrMgrMap = new HashMap<Integer, Pair<String, String>>();
		for (Map.Entry<String, WorkerManager> e : this.idManagerMap.entrySet()) {
			if (thisWkrMgr != null) {
				logger.info("Initializing worker manager : "
						+ thisWkrMgr.getId());
				thisWkrMgr.initialize(wkrMgrPartitions,
						this.getWorkerMgrThreads(), gp.getPartitionSize(),
						gp.getNumVertices());
			}
			wkrMgrPartitions.clear();
			thisWkrMgr = e.getValue();
			thisWkrMgrId = e.getKey();
			thisWkrMgrName = thisWkrMgr.getHostInfo();

			for (int i = 0; i < numMgrPartitions; i++, partitionCount++) {
				partitionWkrMgrMap.put(partitionCount,
						new Pair<String, String>(thisWkrMgrId, thisWkrMgrName));
				wkrMgrPartitions.add(partitionCount);
			}

		}

		while (partitionCount < numPartitions) {
			partitionWkrMgrMap.put(partitionCount, new Pair<String, String>(
					thisWkrMgrId, thisWkrMgrName));
			wkrMgrPartitions.add(partitionCount);
			partitionCount++;
		}

		thisWkrMgr.initialize(wkrMgrPartitions, this.getWorkerMgrThreads(),
				gp.getPartitionSize(), gp.getNumVertices());

		// Write partition - worker manager map to file
		DataLocator dl = DataLocator.getDataLocator(gp.getPartitionSize());
		dl.writePartitionMap(partitionWkrMgrMap);
		logger.info("Initialized worker managers : ");
		dl.clearSolutions();
		logger.info("Cleared solutions folder");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.ManagerToMaster#endSuperStep(java.lang.String)
	 */
	@Override
	public void endSuperStep(String wkrMgrId) throws RemoteException {
		if (this.activeMgrs > 0) {
			if (idManagerMap.containsKey(wkrMgrId)) {
				logger.info("Worker manager : " + wkrMgrId
						+ " has reported completion of superstep : "
						+ this.getSuperStep());
				this.activeMgrs--;
			}
			if (this.activeMgrs == 0) {
				logger.info("All worker managers reported completion of superstep : "
						+ this.getSuperStep());
				if (this.isCheckPoint()) {
					logger.info("#############################");
					logger.info("Checkpointed data at superstep : "+this.getSuperStep());
					logger.info("#############################");
				}
				setAllDone(true);
			}
		}
	}

	/**
	 * @param b
	 */
	private synchronized void setAllDone(boolean allDone) {
		this.allDone = allDone;

	}

}
