package system;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

/*
 * Represents the physical partition of a graph
 * 
 */
public class GraphPartition {

	private List<Vertex> listOfVertices;
	private String partitionFile;
	private String vertexClassName;

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

	public GraphPartition(List<Vertex> listOfVertices) {
		this.listOfVertices = listOfVertices;

	}

	// Returns the partiton file using which this partition
	// was initially populated.
	public String getPartitionFile() {
		return this.partitionFile;
	}

	public GraphPartition(String inputFile, String vertexClassName)
			throws IOException, IllegalInputException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		this.vertexClassName = vertexClassName;
		this.partitionFile = inputFile;
		this.listOfVertices = readFromFile(inputFile);

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
			aNewVertex.initialize(line);
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

}
