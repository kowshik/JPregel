/**
 * 
 */
package system;

import java.io.Serializable;

/**
 * Represents messages that are communicated among Vertices at the end of a superstep.
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3147922297024936878L;
	private int sourceVertexID;
	
	/**
	 *
	 * @return  Returns the source vertex ID sending the message
	 */
	public int getSourceVertexID() {
		return sourceVertexID;
	}
	
	private int destVertexID;
	/**
	 * 
	 * @return  Returns the destination vertex ID receiving the message
	 */
	public int getDestVertexID() {
		return destVertexID;
	}
	
	private double msgValue;
	
	/**
	 * 
	 * @return Returns the value contained in the message
	 */
	public double getMessageValue() {
		return msgValue;
	}

	private int superStep;
	
	/**
	 * 
	 * @return Returns the superstep number during which the message was generated
	 */
	public int getSuperStep(){
		return this.superStep;
	}
	
	/**
	 * 
	 * @param sourceVertexID Vertex ID of the source vertex generating this message
	 * @param destVertexID Vertex ID of the destination vertex consuming this message
	 * @param msgValue Value contained in the message
	 * @param superStep Superstep number
	 */
	public Message(int sourceVertexID, int destVertexID, double msgValue, int superStep){
	
		this.sourceVertexID=sourceVertexID;
		this.destVertexID=destVertexID;		
		this.msgValue=msgValue;
		this.superStep=superStep;
	}
	
	/**
	 * Returns a string representation of the message
	 */
	@Override
	public String toString(){
		StringBuffer strBuf=new StringBuffer();
		strBuf.append("\n\nSource Vertex ID : "+getSourceVertexID()+"\n");
		strBuf.append("Destination Vertex ID : "+getDestVertexID()+"\n");
		strBuf.append("Message Value : "+getMessageValue()+"\n");
		strBuf.append("Source Superstep : "+getSuperStep()+"\n\n");
		return strBuf.toString();
		
	}
}
