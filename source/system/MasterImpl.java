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
import java.util.HashMap;
import java.util.Map;
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
	private Map<WorkerManager,WORKER_MANAGER_STATE> activeWorkerMgrs;
	private Logger logger;
	private String id;
	
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR+"master";
	private static final String LOG_FILE_SUFFIX = ".log";
	private static final int PORT_NUMBER = 3672;

	public MasterImpl() throws IOException {
		this.setId("Master");
		initLogger();
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
		File logDir = new File(JPregelConstants.LOG_DIR);
		
		if(!logDir.exists() &&  !logDir.mkdirs()){
			throw new IOException("Can't create root log dir : "+JPregelConstants.LOG_DIR);
		}
		logger = Logger.getLogger(getId());
		//logger.setUseParentHandlers(false);
		Handler logHandle = null;
		try {
			logHandle = new FileHandler(LOG_FILE_PREFIX+LOG_FILE_SUFFIX);
		} catch (SecurityException e) {
			System.err.println("Can't init logger in "+getId());
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Can't init logger in "+getId());
			e.printStackTrace();
		}
		logHandle.setFormatter(new SimpleFormatter());
		logger.addHandler(logHandle);
		logger.info("init "+getId()+" Logger successful");
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
		logger.info("registered worker : " + id);
		this.activeWorkerMgrs.put(aWorkerManager, WORKER_MANAGER_STATE.ACTIVE);
		logger.info("registered state of worker : " + id+" to "+ WORKER_MANAGER_STATE.ACTIVE);
		
	}

	public static void main(String args[]) {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		try {

			MasterToClient master = new MasterImpl();
			Registry registry = LocateRegistry.createRegistry(PORT_NUMBER);
			registry.rebind(MasterToClient.SERVICE_NAME, master);
			System.err.println("Master instance bound");
		} catch (Exception e) {
			System.err.println("Can't bind Master instance");
			e.printStackTrace();

		}
	}
	
	
	
	public int getWorkerMgrsCount(){
		return this.idManagerMap.size();
	}

	
	public int getWorkerMgrThreads(){
		return JPregelConstants.WORKER_MGR_THREADS;
	}
	
	public String getPartitionLocations(){
		File partitionLocations=new File(JPregelConstants.PARTITION_LOCATIONS);
		if(!partitionLocations.exists() && !partitionLocations.mkdirs()){
			
			System.err.println("Unable to create location : "+partitionLocations.getAbsolutePath());
			return null;
		}
		return JPregelConstants.PARTITION_LOCATIONS;
	}
	
	public  String getPartitionFile(int partitionIndex){
		return getPartitionLocations()+"/partition_"+partitionIndex+".dat";
	}

}
