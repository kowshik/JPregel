package system;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;


//Partitions the graph into several chunks
public class GraphPartitioner {
	private String vertexClassName;
	private String graphFile;
	private MasterImpl master;
	private List<GraphPartition> listOfPartitions;

	private Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "graphpartitioner";
	private static final String LOG_FILE_SUFFIX = ".log";

	public GraphPartitioner(String graphFile, final MasterImpl master, String vertexClassName)
			throws IOException {
		this();
		this.vertexClassName=vertexClassName;
		this.graphFile = graphFile;
		this.master = master;
		this.listOfPartitions = new Vector<GraphPartition>();
	}

	private void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(getId(), LOG_FILE_PREFIX
				+ LOG_FILE_SUFFIX);
	}

	/**
	 * @return
	 */
	private String getId() {
		// TODO Auto-generated method stub
		return "GraphPartitioner";
	}

	/**
	 * @throws IOException
	 * 
	 */
	public GraphPartitioner() throws IOException {
		this.initLogger();
	}

	public int partitionGraphs() throws IOException, IllegalInputException,
			DataNotFoundException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		// number of worker managers in the system
		int numWorkers = master.getWorkerMgrsCount();
		
		// number of threads in each worker manager
		int numThreads = master.getWorkerMgrThreads();
		
		// number of lines in the input graph
		int numLines = this.countLines();
		
		logger.info("Lines : " + numLines);

		//Average no. of lines in a graph partition
		int avgPartitionSize = numThreads * numWorkers;
		
		logger.info("Average Partition Size : "+avgPartitionSize);
		BufferedReader buffRdr = new BufferedReader(new FileReader(
				this.graphFile));
		String line = null;
		int thisPartitionSize = 0;
		while (true) {
			thisPartitionSize = 0;
			List<Vertex> listOfVertices = new Vector<Vertex>();
			while (thisPartitionSize < avgPartitionSize
					&& (line = buffRdr.readLine()) != null) {
				thisPartitionSize++;
				Vertex newVertex = (Vertex)(Class.forName(vertexClassName).newInstance());
				newVertex.initialize(line);
				listOfVertices.add(newVertex);
			}

			if (listOfVertices.size() > 0) {
				String newPartitionFile = DataLocator.getDataLocator()
						.getPartitionFile(listOfPartitions.size());
				GraphPartition newPartition = new GraphPartition(listOfVertices);
				logger.info("Dumped : " + listOfVertices.size() + " to file : "
						+ newPartitionFile);
				newPartition.writeToFile(newPartitionFile);
				listOfPartitions.add(newPartition);
				newPartition.freePartition();
			}
			if (line == null) {
				logger.info("Breaking because no lines are left");
				break;
			}
		}
		buffRdr.close();
		return listOfPartitions.size();
	}

	//Returns number of partitions
	public int getNumberOfPartitions(){
		return listOfPartitions.size();
	}
	
	//Counts the line in the input graph
	private int countLines() throws IOException {
		BufferedReader buffRdr = new BufferedReader(new FileReader(
				this.graphFile));

		int count = 0;
		while (buffRdr.readLine() != null) {
			count++;
		}
		buffRdr.close();
		return count;
	}

}
