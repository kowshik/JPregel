/**
 * 
 */
package system;

import java.rmi.RemoteException;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public interface ManagerToMaster  extends java.rmi.Remote{
	String SERVICE_NAME = "master";

	enum WORKER_MANAGER_STATE {
		ACTIVE, INACTIVE
	};
	
	void register(WorkerManager aWorkerManager, String id)
			throws RemoteException;
}
