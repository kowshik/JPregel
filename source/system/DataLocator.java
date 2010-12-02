/**
 * 
 */
package system;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class DataLocator {

	private static DataLocator aDataLocator;

	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR+"datalocator";
	private static final String LOG_FILE_SUFFIX = ".log";

	private Logger logger;
	
	private DataLocator() throws IOException {
		initLogger();
	}

	
	private void initLogger() throws IOException {
		this.logger=JPregelLogger.getLogger(getId(), LOG_FILE_PREFIX+LOG_FILE_SUFFIX);
	}
	
	
	private String getId() {
	
		return "DataLocator";
	}


	public static DataLocator getDataLocator() throws IOException {
		if (aDataLocator == null) {
			aDataLocator = new DataLocator();
		}
		return aDataLocator;
	}

	public String getPartitionFile(int partitionNumber) throws DataNotFoundException {
		String partitionLocations = getPartitionLocations() ;
		
		return partitionLocations+ "/" + partitionNumber
				+ JPregelConstants.DATA_FILE_EXTENSION;
	}

	public String getPartitionLocations() throws DataNotFoundException {
		File partitionLocations = new File(JPregelConstants.PARTITION_LOCATIONS);
		if (!partitionLocations.exists() && !partitionLocations.mkdirs()) {
			
			String msg = "Unable to create location : "
				+ partitionLocations.getAbsolutePath();
			logger.severe(msg);
			throw new DataNotFoundException(msg);
		}
		return JPregelConstants.PARTITION_LOCATIONS;
	}
}
