package system;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

public class GraphPartitioner {
	private String graphFile;
	private MasterImpl master;
	private List<GraphPartition> listOfPartitions;
	
	public GraphPartitioner(String graphFile, final MasterImpl master){
		this.graphFile=graphFile;
		this.master = master;
		this.listOfPartitions=new Vector<GraphPartition>();
	}
	
	public void partitionGraphs() throws IOException, IllegalInputException{
		//number of workers
		//number of threads
		//number of lines
		
		int numWorkers=master.getWorkerMgrsCount();
		int numThreads=master.getWorkerMgrThreads();
		int numLines = this.countLines();
		
		int avgPartitionSize=numThreads*numWorkers;
	//	int numPartitions=numLines/avgPartitionSize;
		
		BufferedReader buffRdr = new BufferedReader(new FileReader(this.graphFile));
		String line=null;
		int thisPartitionSize=0;
		while((line=buffRdr.readLine())!=null){
			thisPartitionSize=0;
			List<Vertex> listOfVertices=new Vector<Vertex>();
			while(thisPartitionSize<avgPartitionSize){
				thisPartitionSize++;
				Vertex newVertex=new Vertex(line);
				listOfVertices.add(newVertex);
			}
			String newPartitionFile = master.getPartitionFile(listOfPartitions.size());
			GraphPartition newPartition=new GraphPartition(newPartitionFile,listOfVertices);
			newPartition.writeToFile();
			listOfPartitions.add(newPartition);
		}
		buffRdr.close();
		return;
	}

	private int countLines() throws IOException {
		BufferedReader buffRdr = new BufferedReader(new FileReader(this.graphFile));
		
		int count=0;
		while(buffRdr.readLine()!=null){
			count++;
		}
		
		return count;
	}
	
	public static void main(String[] args) throws IOException, IllegalInputException{
		String file = "/cs/student/kowshik/jpregel/testinput.dat";
		MasterImpl master = new MasterImpl();
		GraphPartitioner gp = new GraphPartitioner(file, master);
		gp.partitionGraphs();
	}
	
	
	
}
