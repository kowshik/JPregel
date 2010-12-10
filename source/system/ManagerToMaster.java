/**
 * 
 */
package system;

import java.rmi.RemoteException;

/**
 * An interface that defines the contract between a Worker Manager and a master
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public interface ManagerToMaster extends java.rmi.Remote {
	String SERVICE_NAME = "master";

	/**
	 * Registers a worker manager with the master
	 * @param aWorkerManager The worker manager to be registered
	 * @param id Unique ID representing the worker manager
	 * @throws RemoteException
	 */
	void register(WorkerManager aWorkerManager, String id)
			throws RemoteException;

	/**
	 * Called by worker managers to end a superstep asynchronously
	 * 
	 * @param wkrMgrId Unique ID of the worker manager ending the superstep
	 * @throws RemoteException
	 */
	void endSuperStep(String wkrMgrId) throws RemoteException;
}
