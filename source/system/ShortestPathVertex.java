/**
 * 
 */
package system;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 *
 */
public class ShortestPathVertex extends Vertex{
	
	private int prevVertex;
	
	public ShortestPathVertex(){
		this.prevVertex=-1;
	}
	
	private boolean isSourceVertex(){
		if(this.getVertexID() == 0){
			return true;
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see system.Vertex#compute()
	 */
	@Override
	public void compute() {
	
		double minDist= isSourceVertex() ? 0 : JPregelConstants.INFINITY;
		int newPrevVertexID=-1;
		for(Message aMsg : this.getMessages()){
			if(aMsg.getMessageValue() < minDist){
				minDist=aMsg.getMessageValue();
				newPrevVertexID=aMsg.getSourceVertexID();
			}
		}
		
		if(minDist < this.getValue()){
			this.prevVertex=newPrevVertexID;
			this.setValue(minDist);
			for(Edge e : this.getEdges()){
				this.sendMessage(e, minDist + e.getCost());
			}
		}
	}
	
	@Override
	public void writeSolution(OutputStream anOutputStream) throws IOException{
		BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(anOutputStream));
		buffWriter.write(""+this.getVertexID()+"->"+this.getValue()+","+this.prevVertex+"\n");		
		buffWriter.close();
	}

	
}
