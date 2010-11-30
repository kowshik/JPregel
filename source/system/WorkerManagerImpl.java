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
import java.util.Random;
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
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR+"worker_";
	private static final String LOG_FILE_SUFFIX = ".log";
	private ManagerToMaster master;

	private void initLogger() throws IOException {
		File logDir = new File(JPregelConstants.LOG_DIR);

		if (!logDir.exists() && !logDir.mkdirs()) {
			throw new IOException("Can't create root log dir : "
					+ JPregelConstants.LOG_DIR);
		}
		logger = Logger.getLogger(id);
		// logger.setUseParentHandlers(false);
		Handler logHandle = null;
		try {
			logHandle = new FileHandler(LOG_FILE_PREFIX + this.getId()
					+ LOG_FILE_SUFFIX);

		} catch (SecurityException e) {
			System.err.println("Can't init logger in " + this.getId());
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Can't init logger in " + this.getId());
			e.printStackTrace();
		}
		logHandle.setFormatter(new SimpleFormatter());
		logger.addHandler(logHandle);
		logger.info("init " + this.getId() + " Logger successful");
	}

	public WorkerManagerImpl(ManagerToMaster master) throws IOException {

		this.master = master;
		try {
			this.setId(InetAddress.getLocalHost().getHostName() + "_"
					+ WorkerManagerImpl.getRandomChars());
		} catch (UnknownHostException e) {
			System.err.println("Can't set id in worker " + this.getId());
			e.printStackTrace();
		}

		this.initLogger();
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

}
