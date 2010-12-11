/**
 * 
 */
package utility;

import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import system.JPregelConstants;

/**
 * A generic logger that creates java.util.logger objects based on the ID of the
 * consuming class
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class JPregelLogger {
	public static Logger getLogger(String classId, String logFile)
			throws IOException {
		File logDir = new File(JPregelConstants.LOG_DIR);

		if (!logDir.exists() && !logDir.mkdirs()) {
			throw new IOException("Can't create root log dir : "
					+ JPregelConstants.LOG_DIR);
		}
		Logger aLogger = Logger.getLogger(classId);
		aLogger.setUseParentHandlers(false);

		Handler logHandle = null;
		try {
			logHandle = new FileHandler(logFile);

		} catch (SecurityException e) {
			System.err.println("Can't init logger in " + classId);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Can't init logger in " + classId);
			e.printStackTrace();
		}
		logHandle.setFormatter(new SimpleFormatter());
		aLogger.addHandler(logHandle);
		aLogger.info("init " + classId + " Logger successful");

		if (!classId.equals("Master") && !classId.startsWith("Fault")) {
			aLogger.setLevel(Level.SEVERE);
		}

		return aLogger;
	}
}
