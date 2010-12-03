package system;

public class IllegalInputException extends Exception{
	
	public IllegalInputException(String malformedRecord){
		super(malformedRecord);
	}
	
	public String toString(){
		return "Cannot read malformed record "+super.getMessage();
	}

}
