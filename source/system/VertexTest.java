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
public class VertexTest extends Vertex{
	
	
	public VertexTest(){
		
	}

	/* (non-Javadoc)
	 * @see system.Vertex#compute()
	 */
	@Override
	public void compute() {
	
		double maxVal=this.getValue();
		boolean newMaxVal=false;
		for(Message m : this.getMessages()){
			if(m.getMessageValue() > maxVal){
				maxVal=m.getMessageValue();
				newMaxVal=true;
			}
		}
		this.setValue(maxVal);
		if(newMaxVal || this.getSuperStep()==JPregelConstants.FIRST_SUPERSTEP){
			for(Edge e : this.getEdges()){
				this.sendMessage(e, this.getValue());
			}
		}
	}
	
	@Override
	public void writeSolution(OutputStream anOutputStream) throws IOException{
		BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(anOutputStream));
		buffWriter.write(""+this.getVertexID()+"->"+this.getValue()+"\n");		
		buffWriter.close();
	}

	
}
