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
public interface WorkerManager extends java.rmi.Remote  {
	
	String getId() throws RemoteException;
	String getHostInfo() throws RemoteException;
	
	void initialize(List< Integer > partitionNumbers, int numWorkers, int partitionSize, int totalVertices) throws RemoteException;
	
	void beginSuperStep(int superStepNumber, boolean isCheckPoint) throws RemoteException;
	
	void writeSolutions() throws RemoteException;
	
	void isAlive() throws RemoteException;
}
