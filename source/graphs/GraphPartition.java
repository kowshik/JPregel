package graphs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import system.DataLocator;
import system.Message;
import system.Worker;
import api.Vertex;
import exceptions.DataNotFoundException;
import exceptions.IllegalInputException;

/**
 * Represents the physical partition of a graph in memory
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class GraphPartition implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7131394295735085039L;

	// transient----------
	private transient Worker aWorker;
	private transient DataLocator aDataLocator;
	// --------------------

	private List<Vertex> listOfVertices;
	private String partitionFile;
	private String vertexClassName;
	private int partitionID;

	public int getPartitionID() {
		return this.partitionID;
	}

	/**
	 * Converts the partition into a String representation. Each line in the
	 * string representation is obtained by calling the vertex's toString()
	 * method
	 */
	@Override
	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		for (Vertex v : listOfVertices) {
			strBuf.append(v.toString());
			strBuf.append("\n");
		}
		return strBuf.toString();

	}

	/**
	 * 
	 * @param partitionID
	 *            A unique number to represent this partition
	 * @param listOfVertices
	 *            List of vertices assigned to this partition
	 */
	public GraphPartition(int partitionID, List<Vertex> listOfVertices) {
		this.partitionID = partitionID;
		this.listOfVertices = listOfVertices;
		for (Vertex v : listOfVertices) {
			v.setGraphPartition(this);
		}

	}

	/**
	 * 
	 * @return Returns the partiton file using which this partition was
	 *         initially populated.
	 */
	public String getPartitionFile() {
		return this.partitionFile;
	}

	public GraphPartition(int partitionID, String inputFile,
			String vertexClassName, Worker aWorker, DataLocator aDataLocator)
			throws IOException, IllegalInputException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		this(inputFile, vertexClassName);
		this.aDataLocator = aDataLocator;
		this.aWorker = aWorker;
		this.partitionID = partitionID;
		this.listOfVertices = readFromFile(inputFile);
	}

	/**
	 * 
	 * @param inputFile
	 *            File containing an adjacency list representation of the graph
	 * @param vertexClassName
	 *            Name of the vertex class provided by the application
	 *            programmer
	 * @throws IOException
	 * @throws IllegalInputException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public GraphPartition(String inputFile, String vertexClassName)
			throws IOException, IllegalInputException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		this.vertexClassName = vertexClassName;
		this.partitionFile = inputFile;

	}

	/**
	 * Reads the physical partition (in adjacency list format) from file and
	 * caches it. Each line in the file is a record in an adjacency list.
	 * 
	 * @param inputFile File containing an adjacency list representation of the graph
	 * @return A list of vertices representing the partition
	 * @throws IOException
	 * @throws IllegalInputException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public List<Vertex> readFromFile(String inputFile) throws IOException,
			IllegalInputException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		BufferedReader buffReader = new BufferedReader(
				new FileReader(inputFile));

		String line = null;
		List<Vertex> listOfVertices = new Vector<Vertex>();
		while ((line = buffReader.readLine()) != null) {
			Vertex aNewVertex = (Vertex) (Class.forName(vertexClassName)
					.newInstance());
			aNewVertex.initialize(line, this);
			String vtxSolnFile = aDataLocator.getVertexFile(aNewVertex
					.getVertexID());
			aNewVertex.setSolutionFile(vtxSolnFile);
			listOfVertices.add(aNewVertex);
		}

		buffReader.close();
		return listOfVertices;
	}

	/**
	 * Serializes the graph to a partition file in adjacency list format
	 * @param outputFile File where the output should be written
	 * @throws IOException
	 */
	public void writeToFile(String outputFile) throws IOException {
		BufferedWriter buffWriter = new BufferedWriter(new FileWriter(
				outputFile));

		for (Vertex v : listOfVertices) {
			String vertexStr = v.toString();
			buffWriter.write(vertexStr + "\n");
		}

		buffWriter.close();

	}

	/*
	 * Frees the cached partition.
	 */
	public void freePartition() {
		this.listOfVertices = null;
	}

	/**
	 * Returns the list of vertices of this partition
	 * @return
	 */
	public List<Vertex> getVertices() {
		return this.listOfVertices;
	}

	/**
	 * Propagates a message to a workers
	 * @param msg
	 */
	public void send(Message msg) {
		aWorker.send(msg);
	}

	/**
	 * Returns the superstep number
	 * @return
	 */
	public int getSuperStep() {
		return aWorker.getSuperStep();
	}

	/**
	 * Returns the total number of vertices in the input graph
	 * @return
	 */
	public int getTotalNumVertices() {
		return aWorker.getNumVertices();
	}
	
	/**
	 * Writes solutions generaated by each vertex to its corresponding output file
	 * @throws IOException
	 */

	public void writeSolutions() throws IOException {
		for (Vertex v : getVertices()) {
			v.writeSolution();
		}
	}

	/**
	 * 
	 * Saves the state of the partition to disk
	 * 
	 * @throws IOException
	 * @throws DataNotFoundException
	 * 
	 */
	public void saveState() throws IOException, DataNotFoundException {

		String chkPointFile = aDataLocator.getCheckpointFile(getSuperStep(),
				getPartitionID());
		FileOutputStream fos = null;
		ObjectOutputStream out = null;

		fos = new FileOutputStream(chkPointFile);
		out = new ObjectOutputStream(fos);
		out.writeObject(this);
		fos.close();
		out.close();

		String chkPointFlag = aDataLocator.getCheckpointFlag(getSuperStep(),
				getPartitionID());

		File chkPointFlagFile = new File(chkPointFlag);
		if (chkPointFlagFile.exists()) {
			chkPointFlagFile.delete();
		}
		if (!chkPointFlagFile.createNewFile()) {
			String msg = "Unable to create location : "
					+ chkPointFlagFile.getAbsolutePath();
			throw new DataNotFoundException(msg);
		}
	}

	/**
	 * Clears the vertex message queues
	 */
	public void clearVertexQueues() {
		for (int index = 0; index < listOfVertices.size(); index++) {
			Vertex v = listOfVertices.get(index);
			v.clearMessageQueue();
		}

	}

	/**
	 * @param Sets the worker thread that controls this partition
	 */
	public void setWorker(Worker aWorker) {
		this.aWorker = aWorker;

	}

	/**
	 * @param Sets the DataLocator for this partition
	 */
	public void setDataLocator(DataLocator aDataLocator) {
		this.aDataLocator = aDataLocator;
	}

}
