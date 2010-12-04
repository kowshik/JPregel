/**
 * 
 */
package system;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

import system.Worker.WorkerState;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class Worker implements Runnable {

	private Thread t;
	private List<GraphPartition> listOfPartitions;
	private String id;
	private Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR+"worker_";
	private static final String LOG_FILE_SUFFIX = ".log";
	public static enum WorkerState {EXECUTE, STOP};
	private WorkerManager mgr;
	private String vertexClassName;
	private WorkerState workerState;
	
	private Worker(WorkerManager mgr, String vertexClassName) throws IOException {
		this.workerState=WorkerState.STOP;
		this.mgr=mgr;
		this.vertexClassName=vertexClassName;
		this.listOfPartitions=new Vector<GraphPartition>();
		this.setId(mgr.getId() + "_" + Worker.getRandomChars());
		this.initLogger();
		t=new Thread(this,this.getId());
		t.start();
	}

	/**
	 * @throws IOException 
	 * 
	 */
	private void initLogger() throws IOException {
		this.logger=JPregelLogger.getLogger(this.getId(), LOG_FILE_PREFIX+this.getId()+LOG_FILE_SUFFIX);
		
	}

	/**
	 * @param string
	 */
	private void setId(String id) {
		this.id = id;

	}

	public String getId() {
		// TODO Auto-generated method stub
		return id;
	}

	public Worker(List<Integer> partitionNumers, WorkerManager mgr, String vertexClassName) throws DataNotFoundException,
			IOException, IllegalInputException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		this(mgr,vertexClassName);
		logger.info("Worker : "+this.getId()+" received partitions : "+partitionNumers);
		DataLocator theDataLocator = DataLocator.getDataLocator();
		for (Integer partitionNumber : partitionNumers) {
			String partitionFile = theDataLocator
					.getPartitionFile(partitionNumber);
			GraphPartition aGraphPartition = new GraphPartition(partitionFile,this.vertexClassName);
			logger.info("Worker : "+this.getId()+" initialized partition : "+partitionFile);
			this.listOfPartitions.add(aGraphPartition);
		}
		
		logger.info("Initialized worker with "+this.listOfPartitions.size()+" partitions");
		logger.info("Partitions are : \n\n"+this.listOfPartitions);
		
	}

	@Override
	public void run() {
		while(true){
			if(this.getState() == WorkerState.EXECUTE){
				for(GraphPartition gPartition : this.listOfPartitions){
					for(Vertex v : gPartition.getVertices()){
						v.compute(null);
						logger.info("Executed compute() on vertex : "+v.toString());
					}
				}
				this.setState(WorkerState.STOP);
			}
		}

	}

	/**
	 * @return
	 */
	public synchronized WorkerState getState() {
		return this.workerState;
	}
	
	/**
	 * @return
	 */
	public synchronized void setState(WorkerState newState) {
		this.workerState = newState;
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
	
	public String toString(){
		StringBuffer strBuf = new StringBuffer();
		strBuf.append("Worker ID : "+this.getId()+"\n");
		strBuf.append("Partitions : "+this.listOfPartitions.size()+"\n\n");
		return strBuf.toString();
	}

}
