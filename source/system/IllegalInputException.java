package system;

public class IllegalInputException extends Exception{
	
	public IllegalInputException(String malformedRecord){
		super(malformedRecord);
	}
	
	public String toString(){
		return "Vertex cannot read malformed record "+super.getMessage();
	}

}
