/**
 * 
 */
package exceptions;

/**
 * 
 * Thrown when no resources are available to the Master to execute a task
 * submitted by the application programmer
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class NoResourcesException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9004294713180725705L;

	public NoResourcesException(String msg) {
		super(msg);
	}
}
