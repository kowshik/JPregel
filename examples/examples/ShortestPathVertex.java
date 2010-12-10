/**
 * 
 */
package examples;

import graphs.Edge;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import system.JPregelConstants;
import system.Message;
import api.Vertex;

/**
 * 
 * Calculates the single source shortest path from the source vertex to every
 * other vertex in the graph based in Dijkstra's shortest path algorithm
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class ShortestPathVertex extends Vertex {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4877455216665842419L;
	
	/*
	 * The vertex previous to the current vertex in the shortest path solution
	 */
	private int prevVertex;

	public ShortestPathVertex() {
		this.prevVertex = -1;
	}

	private boolean isSourceVertex() {
		if (this.getVertexID() == 0) {
			return true;
		}
		return false;
	}

	/*
	 * Implementation of compute() for Dijkstra's shortest path
	 */
	@Override
	public void compute() {

		double minDist = isSourceVertex() ? 0 : JPregelConstants.INFINITY;
		int newPrevVertexID = -1;
		for (Message aMsg : this.getMessages()) {
			if (aMsg.getMessageValue() < minDist) {
				minDist = aMsg.getMessageValue();
				newPrevVertexID = aMsg.getSourceVertexID();
			}
		}

		if (minDist < this.getValue()) {
			this.prevVertex = newPrevVertexID;
			this.setValue(minDist);
			for (Edge e : this.getEdges()) {
				this.sendMessage(e, minDist + e.getCost());
			}
		}
	}
	
	/*
	 * Write the solution to an output stream
	 */

	@Override
	public void writeSolution(OutputStream anOutputStream) throws IOException {
		BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(
				anOutputStream));
		buffWriter.write("" + this.getVertexID() + "->" + this.getValue() + ","
				+ this.prevVertex + "\n");
		buffWriter.close();
	}

}
