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
public interface MessageSpooler {
	void queueMessage(Message msg) throws RemoteException;
	int getQueueSize() throws RemoteException;
}
