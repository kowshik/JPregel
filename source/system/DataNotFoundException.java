package system;

public class DataNotFoundException extends Exception{
	
	public DataNotFoundException(String msg){
		super(msg);
	}
	
	public String toString(){
		return "Problems in DataLocator : "+super.getMessage();
	}

}
