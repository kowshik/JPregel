/**
 * 
 */
package system;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public class IllegalMessageException extends Exception {
	
	public IllegalMessageException(Message msg, String wkrMgrId){
		super("Message : "+msg.toString()+"\nwas delivered to the wrong worker manager : "+wkrMgrId);		
		
	}
	
	public String toString(){		
		return this.getMessage();
	}
}
