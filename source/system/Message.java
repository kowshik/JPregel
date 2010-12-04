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
	public String sourceVertexID;
	
	public String getSourceVertexID() {
		return sourceVertexID;
	}


	public int msgValue;
	
	public int getMessageValue() {
		return msgValue;
	}


	public Message(String sourceVertexID, int msgValue){
		if(sourceVertexID==null){
			throw new NullPointerException("sourceVertexID = null");
		}
		this.sourceVertexID=sourceVertexID;
		
		this.msgValue=msgValue;
	}
	
	
	public String toString(){
		return this.getSourceVertexID()+":"+this.getMessageValue();
	}
}
