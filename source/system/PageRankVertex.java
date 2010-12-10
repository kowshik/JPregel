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
public class PageRankVertex extends Vertex{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4444855908940126630L;

	public PageRankVertex(){
		
	}

	/* (non-Javadoc)
	 * @see system.Vertex#compute()
	 */
	@Override
	public void compute() {
		
		if(this.getSuperStep()>=1){
			double sum=0;
			for(Message m : this.getMessages()){
				sum+=m.getMessageValue();
			}
			double newPageRank=0.15/this.getTotalNumVertices()+0.85*sum;
			this.setValue(newPageRank);
		}
		
		if(this.getSuperStep()< 30){
			int numEdges=this.getEdges().size();
			for(Edge e : this.getEdges()){
				this.sendMessage(e, this.getValue()/numEdges);
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
