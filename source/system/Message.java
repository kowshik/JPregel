/**
 * 
 */
package system;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public class Message {
	public int sourceVertexID;
	
	public int getSourceVertexID() {
		return sourceVertexID;
	}
	
	private int destVertexID;
	public int getDestVertexID() {
		return destVertexID;
	}
	
	public int msgValue;
	
	
	public int getMessageValue() {
		return msgValue;
	}

	public int superStep;
	public int getSuperStep(){
		return this.superStep;
	}
	
	public Message(int sourceVertexID, int destVertexID, int msgValue, int superStep){
	
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
