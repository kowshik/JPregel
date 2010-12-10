package system;


//Exception thrown when required data is not found by the DataLocator at runtime
public class DataNotFoundException extends Exception{
	
	public DataNotFoundException(String msg){
		super(msg);
	}
	
	public String toString(){
		return "Problems in DataLocator : "+super.getMessage();
	}

}
