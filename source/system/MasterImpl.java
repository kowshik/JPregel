/**
 * 
 */
package system;

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
	private Logger logger;
	private String id;
	private static final String LOG_FILE_PREFIX = "/cs/student/kowshik/jpregel_logs/master";
	private static final String LOG_FILE_SUFFIX = ".log";
	private static final int PORT_NUMBER = 3672;

	public MasterImpl() throws RemoteException {
		this.setId("Master");
		initLogger();
		this.idManagerMap = new HashMap<String, WorkerManager>();
	}

	

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}
	
	
	private void initLogger() {
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


}
