package exceptions;


/**
 * Exception thrown when required data is not found by the DataLocator at runtime
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public class DataNotFoundException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1349045423800962549L;

	public DataNotFoundException(String msg){
		super(msg);
	}
	
	public String toString(){
		return "Problems in DataLocator : "+super.getMessage();
	}

}
