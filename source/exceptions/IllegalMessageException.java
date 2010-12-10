/**
 * 
 */
package exceptions;

import system.Message;

/**
 * Thrown y Worker Managers bwhen messages are not correctly delivered
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class IllegalMessageException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7845355024569065243L;

	public IllegalMessageException(Message msg, String wkrMgrId) {
		super("Message : " + msg.toString()
				+ "\nwas delivered to the wrong worker manager : " + wkrMgrId);

	}

	public String toString() {
		return this.getMessage();
	}
}
