package system;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

public class GraphPartition {
	private String outputFile;
	private List<Vertex> listOfVertices;
	private boolean isFileWritten;

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public GraphPartition(String outputFile, List<Vertex> listOfVertices) {
		this.listOfVertices = listOfVertices;
		this.outputFile = outputFile;
		this.isFileWritten= false;
	}

	public void overWriteFile() throws IOException{
		BufferedWriter buffWriter = new BufferedWriter(new FileWriter(
				outputFile));

		for (Vertex v : listOfVertices) {
			String vertexStr = v.toString();
			buffWriter.write(vertexStr + "\n");
		}

		buffWriter.close();
		this.listOfVertices = null;
		this.isFileWritten=true;
		
	}
	public boolean writeToFile() throws IOException {
		if (!isFileWritten) {
			overWriteFile();
			return true;
		}
		return false;
	}
	

	public boolean isFileWritten() {
		return isFileWritten;
	}
}
