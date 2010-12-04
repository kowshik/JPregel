package system;



public class IllegalClassException extends Exception{
	
	public IllegalClassException(String className){
		super(className);
	}
	
	public String toString(){
		return "Client class : "+this.getMessage()+" should extend "+Vertex.class;
	}

}
