package api;

import exceptions.IllegalInputException;
import graphs.Edge;
import graphs.GraphPartition;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import system.JPregelConstants;
import system.Message;

/**
 * 
 * Abstracts a vertex in any input graph, and defines methods that can be
 * overrided by the application programmer to define the computation tied to
 * every vertex in any superstep of execution.
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public abstract class Vertex implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7498169364791651592L;
	private int vertexID;
	public static final String vertexToEdgesSep = "->";
	public static final String edgesSep = ",";
	public List<Message> msgs;

	private List<Edge> outgoingEdges;
	private GraphPartition gPartition;
	private String solutionFile;
	private double value;

	/**
	 * Returns the vertex ID that uniquely identifies this vertex during runtime
	 * 
	 * @return
	 */
	public int getVertexID() {
		return vertexID;
	}

	/**
	 * Sets the vertex ID
	 * 
	 * @param vertexID
	 *            vertex ID to be set
	 */
	public void setVertexID(int vertexID) {
		this.vertexID = vertexID;
	}

	/**
	 * 
	 * @return Value contained by this vertex
	 */
	public double getValue() {
		return value;
	}

	/**
	 * 
	 * @param value
	 *            New value for this vertex
	 */
	public void setValue(double value) {
		this.value = value;
	}

	/**
	 * 
	 * @return Path of the file to which the vertex's solution will be
	 *         serialized after the current graph problem is solved
	 */
	public String getSolutionFile() {
		return solutionFile;
	}

	/**
	 * 
	 * @param solutionFile
	 *            Sets the path of the file to which the vertex's solution will
	 *            be serialized after the current graph problem is solved
	 */
	public void setSolutionFile(String solutionFile) {
		this.solutionFile = solutionFile;
	}

	/**
	 * 
	 * @param aPartition
	 *            Sets the graph partition that contains this vertex
	 */
	public void setGraphPartition(GraphPartition aPartition) {
		this.gPartition = aPartition;
	}

	/**
	 * 
	 * @return The graph partition that contains this vertex
	 */
	public GraphPartition getGraphPartition() {
		return this.gPartition;
	}

	/**
	 * Initializes the vertex with an ID, value and a list of outgoing edges
	 * 
	 * @param vertexID
	 * @param value
	 * @param outgoingEdges
	 */
	public void initialize(int vertexID, Integer value, List<Edge> outgoingEdges) {
		this.vertexID = vertexID;
		this.value = value;
		this.outgoingEdges = outgoingEdges;
		if (outgoingEdges == null) {
			this.outgoingEdges = new Vector<Edge>();
		}
	}

	/**
	 * Initializes the vertex with the adjacency list record representing it and
	 * the partition that owns it
	 * 
	 * @param adjacencyListRecord
	 * @param partition
	 * @throws IllegalInputException
	 */
	public void initialize(String adjacencyListRecord, GraphPartition partition)
			throws IllegalInputException {
		this.gPartition = partition;
		this.solutionFile = null;
		initialize(adjacencyListRecord);
	}

	/**
	 * Initializes the vertex with the adjacency list record representing it
	 * 
	 * @param adjacencyListRecord
	 * @throws IllegalInputException
	 */
	public void initialize(String adjacencyListRecord)
			throws IllegalInputException {
		this.msgs = new LinkedList<Message>();
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

	/**
	 * Returns a string representation of this vertex
	 */
	@Override
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

	/**
	 * 
	 * @return The current superstep number
	 */
	public int getSuperStep() {
		return this.gPartition.getSuperStep();

	}

	/**
	 * 
	 * @return List of outgoing edges
	 */

	public List<Edge> getEdges() {
		return this.outgoingEdges;
	}

	/**
	 * 
	 * @param edges
	 *            Sets the list of outgoing edges
	 */

	public void setEdges(List<Edge> edges) {
		this.outgoingEdges = edges;
	}

	/**
	 * Queues a message to be sent to a remote vertex
	 * 
	 * @param e
	 *            Outgoing edge on which this message must be sent
	 * @param msgValue
	 *            Value of the message
	 */
	public void sendMessage(Edge e, double msgValue) {
		Message aMsg = new Message(e.getSourceVertexID(), e.getDestVertexID(),
				msgValue, this.getSuperStep());
		this.gPartition.send(aMsg);
	}

	/**
	 * This method must be overriden by the application programmer to define the
	 * computation tied to this vertex
	 */
	public abstract void compute();

	/**
	 * 
	 * @return Total number of vertices in the input graph
	 */
	public int getTotalNumVertices() {
		return this.gPartition.getTotalNumVertices();
	}

	/**
	 * Queues a message in the vertex's message queue
	 * 
	 * @param msg
	 *            Message to be queued
	 */
	public void queueMessage(Message msg) {
		this.msgs.add(msg);

	}

	/**
	 * 
	 * @return All messages currently present in the vertex's queue
	 * 
	 */
	public List<Message> getMessages() {
		return msgs;

	}

	/**
	 * Initiates the computation by calling the compute() method. Clears all
	 * messages in the message queue once the computation is over.
	 */
	public void initCompute() {
		compute();
		this.msgs.clear();
	}

	/**
	 * Initiates writing of the solution by calling the writeSolution() method
	 * defined by the application programmer.
	 * 
	 * @param anOutputStream
	 * @throws IOException
	 */
	public void writeSolution() throws IOException {
		OutputStream anOutputStream = new FileOutputStream(solutionFile);
		writeSolution(anOutputStream);
	}

	/**
	 * This method must be override by the application programmer to dump the
	 * solution in an appropriate format upon completion of the graph problem
	 * 
	 * @param anOutputStream
	 *            Output stream to which the solution should be written
	 * @throws IOException
	 */
	public abstract void writeSolution(OutputStream anOutputStream)
			throws IOException;

	/**
	 * Clears the message queue
	 */
	public void clearMessageQueue() {
		this.msgs.clear();

	}
	

}
