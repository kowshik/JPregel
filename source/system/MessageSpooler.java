/**
 * 
 */
package system;

import java.rmi.RemoteException;

/**
 * 
 * An interface that models every Worker Manager as a spooler of incoming messages for the next superstep
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public interface MessageSpooler extends java.rmi.Remote {
	/**
	 * Queues messages to be executed in the next superstep
	 * @param msg Message to be queued
	 * @throws RemoteException
	 */
	void queueMessage(Message msg) throws RemoteException;
	
	/**
	 * 
	 * @return Returns the size of the queue for the next superstep at this instant
	 * @throws RemoteException
	 */
	boolean isQueueEmpty() throws RemoteException;
}
