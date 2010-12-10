package exceptions;

import api.Vertex;



/**
 * Thrown when the class submitted by the application programmer does not :
 * 
 * (A) Extend class Vertex
 * (B) Define a default constructor
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */

public class IllegalClassException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3337407014225093554L;

	public IllegalClassException(String className){
		super(className);
	}
	
	public String toString(){
		return "Client class : "+this.getMessage()+" should extend "+Vertex.class;
	}

}
