package system;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

public class GraphPartition {

	private List<Vertex> listOfVertices;
	private String partitionFile;
	
	public String toString(){
		StringBuffer strBuf=new StringBuffer();
		for(Vertex v : listOfVertices){
			strBuf.append(v.toString());
			strBuf.append("\n");
		}
		return strBuf.toString();
		
	}
	
	public GraphPartition(List<Vertex> listOfVertices) {
		this.listOfVertices = listOfVertices;

	}

	
	public String getPartitionFile(){
		return this.partitionFile;
	}
	public GraphPartition(String inputFile) throws IOException, IllegalInputException {
		
		this.partitionFile = inputFile;
		this.listOfVertices = readFromFile(inputFile);

	}

	public List<Vertex> readFromFile(String inputFile) throws IOException,
			IllegalInputException {
		BufferedReader buffReader = new BufferedReader(
				new FileReader(inputFile));

		String line = null;
		List<Vertex> listOfVertices = new Vector<Vertex>();
		while ((line = buffReader.readLine()) != null) {
			Vertex aNewVertex = new Vertex(line);
			listOfVertices.add(aNewVertex);
		}

		buffReader.close();
		return listOfVertices;
	}

	public void writeToFile(String outputFile) throws IOException {
		BufferedWriter buffWriter = new BufferedWriter(new FileWriter(
				outputFile));

		for (Vertex v : listOfVertices) {
			String vertexStr = v.toString();
			buffWriter.write(vertexStr + "\n");
		}

		buffWriter.close();

	}

	public void freePartition() {
		this.listOfVertices = null;
	}

}
