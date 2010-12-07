package system;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

public abstract class Vertex {

	private int vertexID;
	public static final String vertexToEdgesSep = "->";
	public static final String edgesSep = ",";
	public List<Message> msgs;
	protected Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "vertex_";
	private static final String LOG_FILE_SUFFIX = ".log";

	protected void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(this.getVertexID()+"", LOG_FILE_PREFIX
				+ this.getVertexID() + LOG_FILE_SUFFIX);

	}

	public int getVertexID() {
		return vertexID;
	}

	public void setVertexID(int vertexID) {
		this.vertexID = vertexID;
	}

	private double value;

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	private List<Edge> outgoingEdges;
	private GraphPartition gPartition;
	private String solutionFile;

	public String getSolutionFile() {
		return solutionFile;
	}

	public void setSolutionFile(String solutionFile) {
		this.solutionFile = solutionFile;
	}

	public void setGraphPartition(GraphPartition aPartition) {
		this.gPartition = aPartition;
	}

	public GraphPartition getGraphPartition() {
		return this.gPartition;
	}

	public void initialize(int vertexID, Integer value, List<Edge> outgoingEdges) {
		this.vertexID = vertexID;
		this.value = value;
		this.outgoingEdges = outgoingEdges;
		if (outgoingEdges == null) {
			this.outgoingEdges = new Vector<Edge>();
		}
	}

	public void initialize(String adjacencyListRecord, GraphPartition partition)
			throws IllegalInputException {
		this.gPartition = partition;
		this.solutionFile=null;
		initialize(adjacencyListRecord);
	}

	public void initialize(String adjacencyListRecord)
			throws IllegalInputException {
		this.msgs=new LinkedList<Message>();
		// Example : A->B:3,C:5,D:25

		/*
		 * Example : A->B:3,C:5,D:25 is split into vertexToEdges[0] = A
		 * vertexToEdges[1] = B:3,C:5,D:25
		 */
		String[] vertexToEdges = adjacencyListRecord.split(vertexToEdgesSep);
		if (vertexToEdges.length != 2) {
			throw new IllegalInputException(adjacencyListRecord);
		}
		int vertexID = -1;

		try {
			vertexID = Integer.parseInt(vertexToEdges[0]);

		} catch (NumberFormatException e) {
			throw new IllegalInputException(adjacencyListRecord);
		}

		if (vertexID < 0) {
			throw new IllegalInputException(adjacencyListRecord);
		}

		this.setVertexID(vertexID);
		this.setValue(JPregelConstants.INFINITY);

		/*
		 * Example : B:3,C:5,D:25 is split into outgoingVertices[0] = B:3
		 * outgoingVertices[1] = C:5 outgoingVertices[2] = D:25
		 */
		String[] outgoingEdges = vertexToEdges[1].split(edgesSep);
		this.outgoingEdges = new Vector<Edge>();
		for (String edgeDetail : outgoingEdges) {
			Edge e = new Edge(this.getVertexID(), edgeDetail);
			this.outgoingEdges.add(e);
		}
	}

	public String toString() {
		String str = this.getVertexID() + vertexToEdgesSep;

		boolean firstItemCrossed = false;
		for (Edge e : outgoingEdges) {
			if (firstItemCrossed) {
				str += edgesSep;
			}
			firstItemCrossed = true;
			str += e.toString();
		}

		return str;
	}

	public int getSuperStep() {
		return this.gPartition.getSuperStep();

	}

	public List<Edge> getEdges() {
		return this.outgoingEdges;
	}

	public void setEdges(List<Edge> edges) {
		this.outgoingEdges = edges;
	}

	public void voteToHalt() {

	}

	public void sendMessage(Edge e, double msgValue) {
		Message aMsg = new Message(e.getSourceVertexID(), e.getDestVertexID(),
				msgValue,this.getSuperStep());
		this.gPartition.send(aMsg);
	}

	/*
	 * public static void main(String[] args) throws IllegalInputException{
	 * String a="A->B:25,C:35,D:45,E:34"; String b="B->D:34,E:12,F:56";
	 * 
	 * Vertex va=new Vertex(a); Vertex vb=new Vertex(b);
	 * 
	 * System.err.println(va); System.err.println(vb);
	 * 
	 * }
	 */

	public abstract void compute();

	// TODO implement getNumberOfVertices()
	public int getTotalNumVertices() {
		return this.gPartition.getTotalNumVertices();
	}

	/**
	 * @param msg
	 */
	public void queueMessage(Message msg) {
		this.msgs.add(msg);

	}

	public List<Message> getMessages() {
		return msgs;

	}

	/**
	 * 
	 */
	public void initCompute() {
		compute();
		this.msgs.clear();
	}

	
	/**
	 * @param anOutputStream
	 * @throws IOException
	 */
	public void writeSolution() throws IOException{
		OutputStream anOutputStream = new FileOutputStream(solutionFile);
		writeSolution(anOutputStream);
	}
	
	
	/**
	 * @param anOutputStream
	 * @throws IOException
	 */
	public abstract void writeSolution(OutputStream anOutputStream) throws IOException;
	
}
