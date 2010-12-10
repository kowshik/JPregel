package system;

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

/*
 * Represents the physical partition of a graph
 * 
 */
public class GraphPartition implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7131394295735085039L;
	// transient
	private transient Worker aWorker;
	private transient DataLocator aDataLocator;

	private List<Vertex> listOfVertices;
	private String partitionFile;
	private String vertexClassName;
	private int partitionID;

	public int getPartitionID() {
		return this.partitionID;
	}

	// Converts the partition into a String representation
	// Each line in the string representation is obtained
	// by calling the vertex's toString() method
	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		for (Vertex v : listOfVertices) {
			strBuf.append(v.toString());
			strBuf.append("\n");
		}
		return strBuf.toString();

	}

	public GraphPartition(int partitionID, List<Vertex> listOfVertices) {
		this.partitionID = partitionID;
		this.listOfVertices = listOfVertices;
		for (Vertex v : listOfVertices) {
			v.setGraphPartition(this);
		}

	}

	// Returns the partiton file using which this partition
	// was initially populated.
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

	public GraphPartition(String inputFile, String vertexClassName)
			throws IOException, IllegalInputException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		this.vertexClassName = vertexClassName;
		this.partitionFile = inputFile;

	}

	// Reads the physical partition from file and caches it.
	// Each line in the file is a record in an adjacency list.
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

	// Writes the graph partition to a file.
	public void writeToFile(String outputFile) throws IOException {
		BufferedWriter buffWriter = new BufferedWriter(new FileWriter(
				outputFile));

		for (Vertex v : listOfVertices) {
			String vertexStr = v.toString();
			buffWriter.write(vertexStr + "\n");
		}

		buffWriter.close();

	}

	// Frees the cached partition.
	public void freePartition() {
		this.listOfVertices = null;
	}

	// Returns the list of vertices of this partition
	public List<Vertex> getVertices() {
		return this.listOfVertices;
	}

	public void send(Message msg) {
		aWorker.send(msg);
	}

	public int getSuperStep() {
		return aWorker.getSuperStep();
	}

	public int getTotalNumVertices() {
		return aWorker.getNumVertices();
	}

	public void writeSolutions() throws IOException {
		for (Vertex v : getVertices()) {
			v.writeSolution();
		}
	}

	/**
	 * @throws IOException
	 * @throws DataNotFoundException
	 * 
	 */
	public void saveData() throws IOException, DataNotFoundException {

		String chkPointFile = aDataLocator.getCheckpointFile(getSuperStep(),
				getPartitionID());
		FileOutputStream fos = null;
		ObjectOutputStream out = null;

		fos = new FileOutputStream(chkPointFile);
		out = new ObjectOutputStream(fos);
		out.writeObject(this);
		fos.close();
		out.close();
		
		String chkPointFlag=aDataLocator.getCheckpointFlag(getSuperStep(),
				getPartitionID());
		
		File chkPointFlagFile=new File(chkPointFlag);
		if(chkPointFlagFile.exists()){
			chkPointFlagFile.delete();
		}
		if(!chkPointFlagFile.createNewFile()){
			String msg="Unable to create location : "
				+ chkPointFlagFile.getAbsolutePath();
			throw new DataNotFoundException(msg);
		}
	}

	/**
	 * 
	 */
	public void clearVertexQueues() {
		for (int index=0;index<listOfVertices.size();index++) {
			Vertex v =listOfVertices.get(index);
			v.clearMessageQueue();
		}
		
	}
}
