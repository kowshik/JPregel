/**
 * 
 */
package system;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class Worker implements Runnable {

	private int superStep;
	public int getSuperStep() {
		return superStep;
	}

	public void setSuperStep(int superStep) {
		this.superStep = superStep;
	}

	private Thread t;
	private List<GraphPartition> listOfPartitions;
	private int partitionSize;
	private String id;
	private Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "worker_";
	private static final String LOG_FILE_SUFFIX = ".log";

	public static enum WorkerState {
		EXECUTE, DONE, STOP
	};

	private WorkerManagerImpl mgr;
	private String vertexClassName;
	private WorkerState state;
	private Communicator aCommunicator;
	private int numVertices;
	

	public int getNumVertices() {
		return numVertices;
	}

	private Worker(WorkerManagerImpl mgr, String vertexClassName,
			int partitionSize, Communicator aCommunicator, int numVertices) throws IOException {
		this.state = WorkerState.STOP;
		this.setSuperStep(JPregelConstants.DEFAULT_SUPERSTEP);
		this.mgr = mgr;
		this.vertexClassName = vertexClassName;
		this.partitionSize = partitionSize;
		this.aCommunicator = aCommunicator;
		this.numVertices=numVertices;
		this.listOfPartitions = new Vector<GraphPartition>();
		this.setId(mgr.getId() + "_" + Worker.getRandomChars());
		this.initLogger();
		t = new Thread(this, this.getId());
		t.start();
	}

	/**
	 * @throws IOException
	 * 
	 */
	private void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(this.getId(), LOG_FILE_PREFIX
				+ this.getId() + LOG_FILE_SUFFIX);

	}

	/**
	 * @param string
	 */
	private void setId(String id) {
		this.id = id;

	}

	public String getId() {
		return id;
	}

	public Worker(List<Integer> partitionNumers, int partitionSize,
			WorkerManagerImpl mgr, String vertexClassName,
			Communicator aCommunicator, int numVertices) throws DataNotFoundException,
			IOException, IllegalInputException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		this(mgr, vertexClassName, partitionSize, aCommunicator, numVertices);
		logger.info("Worker : " + this.getId() + " received partitions : "
				+ partitionNumers);
		DataLocator aDataLocator = DataLocator.getDataLocator(partitionSize);
		for (Integer partitionNumber : partitionNumers) {
			String partitionFile = aDataLocator
					.getPartitionFile(partitionNumber);
			GraphPartition aGraphPartition = new GraphPartition(partitionNumber,partitionFile,
					this.vertexClassName, this, aDataLocator);
			logger.info("Worker : " + this.getId()
					+ " initialized partition : " + partitionFile);
			this.listOfPartitions.add(aGraphPartition);
		}

		logger.info("Initialized worker with " + this.listOfPartitions.size()
				+ " partitions");
		logger.info("Partitions are : \n\n" + this.listOfPartitions);

	}

	@Override
	public void run() {
		while (true) {
			
			if (this.getState() == WorkerState.EXECUTE) {
				logger.info("Executing");
				for (GraphPartition gPartition : this.listOfPartitions) {
					for (Vertex v : gPartition.getVertices()) {			
						logger.info("Starting initCompute() on vertex : "
								+ v.toString()+" at superstep : "+getSuperStep());
						v.initCompute();						
						
					}
				}
				this.setState(WorkerState.DONE);
			}
		}

	}

	/**
	 * @return
	 */
	public synchronized WorkerState getState() {
		return this.state;
	}

	/**
	 * @return
	 */
	public synchronized void setState(WorkerState newState) {
		logger.info("Setting worker state to : " + newState);
		this.state = newState;
	}

	/**
	 * 
	 * @return A random name made up of exactly three alphabets
	 */
	public static String getRandomChars() {
		char first = (char) ((new Random().nextInt(26)) + 65);
		char second = (char) ((new Random().nextInt(26)) + 65);
		char third = (char) ((new Random().nextInt(26)) + 65);
		return "" + first + second + third;
	}

	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		strBuf.append("Worker ID : " + this.getId() + "\n");
		strBuf.append("Partitions : " + this.listOfPartitions.size() + "\n\n");
		return strBuf.toString();
	}

	//queue message in communicator for next superstep
	public void send(Message msg) {
		logger.info("Attempting to queue msg : "+msg);
		this.aCommunicator.queueMessage(msg);
	}

	// a hashmap of all vertexID->vertex managed by this worker, so the lookup
	// time to distribute messages will be constant
	public Map<Integer,Vertex> getVertices() {

		Map<Integer,Vertex> idVertexMap = new HashMap<Integer,Vertex>();

		for (GraphPartition gPartition : listOfPartitions) {
			for (Vertex aVertex : gPartition.getVertices()) {
				idVertexMap.put(aVertex.getVertexID(), aVertex);
			}
		}

		return idVertexMap;

	}
	
	
	public void writeSolutions() throws IOException{
		for(GraphPartition gp : this.listOfPartitions){
			gp.writeSolutions();
		}
	}

	/**
	 * @throws DataNotFoundException 
	 * @throws IOException 
	 * 
	 */
	public void saveData() throws IOException, DataNotFoundException {
		for(GraphPartition gp : this.listOfPartitions){
			gp.saveData();
		}
		
	}

	/**
	 * 
	 */
	public void clearVertexQueues() {
		for(GraphPartition gp : this.listOfPartitions){
			gp.clearVertexQueues();
		}
		
	}
}
