/**
 * 
 */
package system;

import java.io.Serializable;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3147922297024936878L;
	public int sourceVertexID;
	
	public int getSourceVertexID() {
		return sourceVertexID;
	}
	
	private int destVertexID;
	public int getDestVertexID() {
		return destVertexID;
	}
	
	public double msgValue;
	
	
	public double getMessageValue() {
		return msgValue;
	}

	public int superStep;
	public int getSuperStep(){
		return this.superStep;
	}
	
	public Message(int sourceVertexID, int destVertexID, double msgValue, int superStep){
	
		this.sourceVertexID=sourceVertexID;
		this.destVertexID=destVertexID;		
		this.msgValue=msgValue;
		this.superStep=superStep;
	}
	
	
	public String toString(){
		StringBuffer strBuf=new StringBuffer();
		strBuf.append("\n\nSource Vertex ID : "+getSourceVertexID()+"\n");
		strBuf.append("Destination Vertex ID : "+getDestVertexID()+"\n");
		strBuf.append("Message Value : "+getMessageValue()+"\n");
		strBuf.append("Source Superstep : "+getSuperStep()+"\n\n");
		return strBuf.toString();
		
	}
}
