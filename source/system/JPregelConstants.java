package system;

import java.io.File;

public interface JPregelConstants {
	Integer INFINITY = -1;
	int WORKER_MGR_THREADS = 2;
	String BASE_DIR = "/cs/student/kowshik/jpregel/";
	String PARTITION_LOCATIONS = BASE_DIR + "/partitions/";
	String PARTITION_MAP = PARTITION_LOCATIONS + "/map.dat";
	String LOG_DIR = BASE_DIR + "/logs/";
	String DATA_FILE_EXTENSION = ".dat";
	String GRAPH_FILE = BASE_DIR + "/graph" + DATA_FILE_EXTENSION;
	int FIRST_SUPERSTEP=1;
	int DEFAULT_SUPERSTEP=0;
}
