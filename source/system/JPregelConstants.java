package system;

import java.io.File;

public interface JPregelConstants {
	Integer INFINITY=-1;
	int WORKER_MGR_THREADS = 1;
	String BASE_DIR="/cs/student/kowshik/jpregel/";
	String PARTITION_LOCATIONS = BASE_DIR+"/partitions/";
	String LOG_DIR = BASE_DIR+"/logs/";
	String DATA_FILE_EXTENSION = ".dat";
	String GRAPH_FILE = BASE_DIR+"/graph"+DATA_FILE_EXTENSION;
	
	
}
