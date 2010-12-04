/**
 * 
 */
package system;

import java.rmi.RemoteException;
import java.util.List;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public interface WorkerManager extends java.rmi.Remote {
	
	void compute() throws RemoteException;
	String getId() throws RemoteException;
	
	void initialize(List< Integer > partitionNumbers, int numWorkers) throws RemoteException;
	
	void beginSuperStep() throws RemoteException;
	
	
}
