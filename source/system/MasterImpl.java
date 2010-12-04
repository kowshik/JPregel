/**
 * 
 */
package system;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
public class MasterImpl extends UnicastRemoteObject implements ManagerToMaster,
		MasterToClient {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7962409918852099855L;

	public Map<String, WorkerManager> idManagerMap;
	private Map<WorkerManager, WORKER_MANAGER_STATE> activeWorkerMgrs;
	private Logger logger;
	private String id;
	private String vertexClassName;

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

	public MasterImpl(String vertexClassName) throws IOException {

		this.setId("Master");
		initLogger();
		this.setVertexClassName(vertexClassName);
		this.idManagerMap = new HashMap<String, WorkerManager>();
		this.activeWorkerMgrs = new HashMap<WorkerManager, WORKER_MANAGER_STATE>();
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

		if (idManagerMap.size() == 3) {
			executeTask();
		}

	}

	public static void main(String args[]) throws IllegalClassException {
		String vertexClassName = args[0];
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

			MasterToClient master = new MasterImpl(vertexClassName);
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

		try {
			initializeWorkerManagers();
			for (Map.Entry<String, WorkerManager> e : this.idManagerMap.entrySet()) {
				e.getValue().beginSuperStep();
			}
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
	 * @throws IOException
	 * @throws DataNotFoundException
	 * @throws IllegalInputException
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * 
	 */
	private void initializeWorkerManagers() throws IOException,
			IllegalInputException, DataNotFoundException, InstantiationException, IllegalAccessException, ClassNotFoundException {

		GraphPartitioner gp = new GraphPartitioner(JPregelConstants.GRAPH_FILE,
				this,this.vertexClassName);
		int numPartitions = gp.partitionGraphs();

		List<Integer> wkrMgrPartitions = new Vector<Integer>();

		int numMgrPartitions = numPartitions / this.getWorkerMgrsCount();

		int partitionCount = 0;
		WorkerManager thisWkrMgr = null;
		for (Map.Entry<String, WorkerManager> e : this.idManagerMap.entrySet()) {
			if (thisWkrMgr != null) {
				logger.info("Initializing worker manager : "
						+ thisWkrMgr.getId());
				thisWkrMgr.initialize(wkrMgrPartitions,
						this.getWorkerMgrThreads());
			}
			wkrMgrPartitions.clear();
			thisWkrMgr = e.getValue();

			for (int i = 0; i < numMgrPartitions; i++, partitionCount++) {
				wkrMgrPartitions.add(partitionCount);
			}

		}

		while (partitionCount < numPartitions) {
			wkrMgrPartitions.add(partitionCount);
			partitionCount++;
		}
		thisWkrMgr.initialize(wkrMgrPartitions, this.getWorkerMgrThreads());

		logger.info("Initialized worker managers : ");

	}

}
