package exceptions;

/**
 * Exception thrown when the input graph contains malformed adjacency list records.
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public class IllegalInputException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6793093160753109312L;

	public IllegalInputException(String malformedRecord){
		super(malformedRecord);
	}
	
	public String toString(){
		return "Cannot read malformed record "+super.getMessage();
	}

}
