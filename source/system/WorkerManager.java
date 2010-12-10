/**
 * 
 */
package system;

import java.rmi.RemoteException;
import java.util.List;

/**
 * Models a remote worker manager executing on any machine in the cluster. Each
 * such manager controls a set of Worker threads and reports to a remote master.
 * The manager also collects incoming messages during the 'Communication' stage
 * of the 'Bulk Synchronous Parallel Model'.
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public interface WorkerManager extends java.rmi.Remote {

	/**
	 * 
	 * @return ID of this worker manager
	 * @throws RemoteException
	 */
	String getId() throws RemoteException;

	/**
	 * 
	 * @return Hostname on which the worker manager is running
	 * @throws RemoteException
	 */
	String getHostInfo() throws RemoteException;

	/**
	 * Used by the Master to initialize the worker manager with graph partitions
	 * and other details about the input graph, before the first superstep is
	 * executed.
	 * 
	 * @param partitionNumbers
	 *            List of partition numbers assigned to this worker manager
	 * @param numWorkers
	 *            Number of workers that can be started by this worker manager
	 * @param partitionSize
	 *            Size of every graph partition (number of lines)
	 * @param totalVertices
	 *            Total number of vertices in the input graph
	 * @throws RemoteException
	 */
	void initialize(List<Integer> partitionNumbers, int numWorkers,
			int partitionSize, int totalVertices) throws RemoteException;

	/**
	 * Commences a particular superstep in the worker manager
	 * 
	 * @param superStepNumber
	 *            the superstep to be commenced (example :1,2,3,..etc.)
	 * @param isCheckPoint
	 *            Is this vertex a check point ?
	 * @throws RemoteException
	 */
	void beginSuperStep(int superStepNumber, boolean isCheckPoint)
			throws RemoteException;

	/**
	 * Dumps solutions from every vertex assigned to this worker manager. This
	 * is called by the Master when all supersteps have been completed.
	 */
	void writeSolutions() throws RemoteException;

	/**
	 * Dummy method used by the Master to ping a worker manager to check if its
	 * alive. Useful for fault tolerance.
	 * 
	 * @throws RemoteException
	 */
	void isAlive() throws RemoteException;

	/**
	 * Stops the current superstep. Used by the Master to stop computations during faults.
	 */
	void stopSuperStep() throws RemoteException;

	/**
	 * Restores the state of the worker manager to a previous check point.
	 * @param lastCheckPoint
	 * @param list
	 */
	void restoreState(int lastCheckPoint, List<Integer> list)
			throws RemoteException;
}
