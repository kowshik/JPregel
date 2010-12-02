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

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class Worker implements Runnable {

	private List<GraphPartition> listOfPartitions;
	private String id;
	private Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR+"worker_";
	private static final String LOG_FILE_SUFFIX = ".log";
	private WorkerManager mgr;
	
	private Worker(WorkerManager mgr) throws IOException {
		this.mgr=mgr;
		this.listOfPartitions=new Vector<GraphPartition>();
		this.setId(mgr.getId() + "_" + Worker.getRandomChars());
		this.initLogger();
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

	public Worker(List<Integer> partitionNumers, WorkerManager mgr) throws DataNotFoundException,
			IOException, IllegalInputException {
		this(mgr);
		logger.info("Worker : "+this.getId()+" received partitions : "+partitionNumers);
		DataLocator theDataLocator = DataLocator.getDataLocator();
		for (Integer partitionNumber : partitionNumers) {
			String partitionFile = theDataLocator
					.getPartitionFile(partitionNumber);
			GraphPartition aGraphPartition = new GraphPartition(partitionFile);
			logger.info("Worker : "+this.getId()+" initialized partition : "+partitionFile);
			this.listOfPartitions.add(aGraphPartition);
		}

		logger.info("Initialized worker with partitions");
		logger.info("Partitions are : \n\n"+this.listOfPartitions);
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

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
