package system;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

public class GraphPartitioner {
	private String graphFile;
	private MasterImpl master;
	private List<GraphPartition> listOfPartitions;

	private Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "graphpartitioner";
	private static final String LOG_FILE_SUFFIX = ".log";

	public GraphPartitioner(String graphFile, final MasterImpl master)
			throws IOException {
		this();
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
			DataNotFoundException {
		// number of workers
		// number of threads
		// number of lines

		int numWorkers = master.getWorkerMgrsCount();
		int numThreads = master.getWorkerMgrThreads();
		int numLines = this.countLines();
		logger.info("Lines : " + numLines);

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
				Vertex newVertex = new Vertex(line);
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

	public int getNumberOfPartitions(){
		return listOfPartitions.size();
	}
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
