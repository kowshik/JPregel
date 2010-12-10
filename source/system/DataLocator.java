/**
 * 
 */
package system;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

//Finds location of partitions and other data at runtime
public class DataLocator {

	/**
	 * 
	 */
	private static final String PARTITION_WKRMGR_SEP = "-";
	private static final String WKRMGR_HOSTNAME_SEP = ",";
	private static DataLocator aDataLocator;
	private int partitionSize;

	public int getPartitionSize() {
		return partitionSize;
	}

	public void setPartitionSize(int partitionSize) {
		this.partitionSize = partitionSize;
	}

	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR
			+ "datalocator";
	private static final String LOG_FILE_SUFFIX = ".log";

	private Logger logger;

	private DataLocator() throws IOException {
		initLogger();
	}

	/**
	 * @param partitionSize
	 * @throws IOException
	 */
	public DataLocator(int partitionSize) throws IOException {
		this();
		this.partitionSize = partitionSize;
	}

	private void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(getId(), LOG_FILE_PREFIX
				+ LOG_FILE_SUFFIX);
	}

	private String getId() {

		return "DataLocator";
	}

	public static DataLocator getDataLocator(int partitionSize)
			throws IOException {
		if (aDataLocator == null) {
			aDataLocator = new DataLocator(partitionSize);
		}
		aDataLocator.setPartitionSize(partitionSize);
		return aDataLocator;
	}

	public String getPartitionFile(int partitionNumber)
			throws DataNotFoundException {
		String partitionLocations = getPartitionLocations();

		return partitionLocations + "/" + partitionNumber
				+ JPregelConstants.DATA_FILE_EXTENSION;
	}

	public String getPartitionLocations() throws DataNotFoundException {
		File partitionLocations = new File(JPregelConstants.PARTITIONS_LOCATION);
		if (!partitionLocations.exists() && !partitionLocations.mkdirs()) {

			String msg = "Unable to create location : "
					+ partitionLocations.getAbsolutePath();
			logger.severe(msg);
			throw new DataNotFoundException(msg);
		}
		return JPregelConstants.PARTITIONS_LOCATION;
	}

	public String getPartitionMap() throws DataNotFoundException {
		this.getPartitionLocations();
		return JPregelConstants.PARTITION_MAP;
	}

	public int getPartitionNumber(int vertexID) {
		return vertexID / getPartitionSize();
	}

	// Writes partition to worker manager ID map to file in the following format
	// :
	// 0:snoopy.cs.ucsb.edu_HJK,snoopy.cs.ucsb.edu
	// 1:snoopy.cs.ucsb.edu_HJK,snoopy.cs.ucsb.edu
	// 2:scooby.cs.ucsb.edu_MNJ,scooby.cs.ucsb.edu
	// 3:scooby.cs.ucsb.edu_MNJ,scooby.cs.ucsb.edu
	// .
	// .
	// .
	// (n-1)th partition:blahblahblah.cs.ucsb.edu_HGJ,blahblahblah.cs.ucsb.edu
	public void writePartitionMap(
			Map<Integer, Pair<String, String>> partitionWkrMgrMap)
			throws IOException, DataNotFoundException {
		BufferedWriter buffWriter = new BufferedWriter(new FileWriter(
				getPartitionMap()));
		for (Map.Entry<Integer, Pair<String, String>> e : partitionWkrMgrMap
				.entrySet()) {
			Pair<String, String> aPair = e.getValue();
			String wkrMgrName = aPair.getFirst();
			String wkrMgrHostName = aPair.getSecond();
			buffWriter.write(e.getKey() + PARTITION_WKRMGR_SEP + wkrMgrName
					+ WKRMGR_HOSTNAME_SEP + wkrMgrHostName + "\n");
		}
		buffWriter.close();

	}

	// Reads data written out by the method writePartitionMap(...)
	public Map<Integer, Pair<String, String>> readPartitionMap()
			throws IOException, DataNotFoundException {
		Map<Integer, Pair<String, String>> partitionWkrMgrMap = new HashMap<Integer, Pair<String, String>>();
		BufferedReader buffReader = new BufferedReader(new FileReader(
				getPartitionMap()));
		String line = null;
		while ((line = buffReader.readLine()) != null) {
			String[] record = line.split(PARTITION_WKRMGR_SEP);
			int partitionNumber = Integer.parseInt(record[0]);
			String wkrMgrInfo = record[1];
			record = wkrMgrInfo.split(WKRMGR_HOSTNAME_SEP);
			String wkrMgrName = record[0];
			String wkrMgrHostName = record[1];
			Pair<String, String> aPair = new Pair<String, String>(wkrMgrName,
					wkrMgrHostName);
			partitionWkrMgrMap.put(partitionNumber, aPair);
		}
		buffReader.close();
		return partitionWkrMgrMap;
	}

	// Returns the output solution file for any vertex
	public String getVertexFile(int vertexID) {
		String vertexFilePath = JPregelConstants.SOLUTIONS_LOCATION + vertexID
				+ JPregelConstants.DATA_FILE_EXTENSION;
		return vertexFilePath;
	}

	public void clearSolutions() throws DataNotFoundException {
		File solnsLocation = new File(JPregelConstants.SOLUTIONS_LOCATION);
		if (solnsLocation.exists()) {
			File[] fileList = solnsLocation.listFiles();
			for (File f : fileList) {
				f.delete();
			}
		} else {
			if (!solnsLocation.mkdirs()) {
				String msg = "Unable to create location : "
						+ solnsLocation.getAbsolutePath();
				throw new DataNotFoundException(msg);
			}
		}
	}

	/**
	 * @param partitionID
	 */
	public String getCheckpointFile(int superstep, int partitionNumber)
			throws DataNotFoundException {
		// TODO Auto-generated method stub
		String chkPointLocation = getCheckpointLocation( superstep);

		return chkPointLocation +"/"+partitionNumber
				+ JPregelConstants.DATA_FILE_EXTENSION;

	}
	
	/*
	 * @param partitionID
	 */
	public String getCheckpointFlag(int superstep, int partitionNumber)
			throws DataNotFoundException {
		// TODO Auto-generated method stub
		String chkPointLocation = getCheckpointLocation(superstep);

		return chkPointLocation + "/"+partitionNumber
				+ JPregelConstants.FLAG_FILE_EXTENSION;

	}


	
	public String getCheckpointLocation(int superstep) throws DataNotFoundException {
		logger.info("Creating checkpoint location : "+JPregelConstants.CHECKPOINT_LOCATION+"/checkpoint-" + superstep);
		File chkPointLocation = new File(JPregelConstants.CHECKPOINT_LOCATION+"/checkpoint-" + superstep);
		if (!chkPointLocation.exists() && !chkPointLocation.mkdirs()) {

			String msg = "Unable to create location : "
					+ chkPointLocation.getAbsolutePath();
			logger.severe(msg);
			throw new DataNotFoundException(msg);
		}
		return chkPointLocation.getAbsolutePath();
	}
}