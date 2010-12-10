package system;


public interface JPregelConstants {
	Double INFINITY = Double.MAX_VALUE;
	int WORKER_MGR_THREADS = 2;
	String BASE_DIR = "/cs/student/kowshik/jpregel/";
	String PARTITIONS_LOCATION = BASE_DIR + "/partitions/";	
	String PARTITION_MAP = PARTITIONS_LOCATION + "/map.dat";
	String SOLUTIONS_LOCATION = BASE_DIR + "/solutions/";
	String LOG_DIR = BASE_DIR + "/logs/";
	String DATA_FILE_EXTENSION = ".dat";
	String FLAG_FILE_EXTENSION = ".flag";
	String GRAPH_FILE = BASE_DIR + "/graph" + DATA_FILE_EXTENSION;
	int FIRST_SUPERSTEP=1;
	int DEFAULT_SUPERSTEP=0;
	int CHECKPOINT_INTERVAL=5;
	String CHECKPOINT_LOCATION = BASE_DIR + "/checkpoints/";;
}
