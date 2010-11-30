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
public interface WorkerManager extends java.rmi.Remote {
	
	void compute() throws RemoteException;
	String getId() throws RemoteException;
	
	
	
}
