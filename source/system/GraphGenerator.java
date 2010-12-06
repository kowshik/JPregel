package system;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

public class GraphGenerator {

	private int numOfVertices;
	private String graphFilePath = JPregelConstants.BASE_DIR + "graph.dat";
	private int maxcost;
	private String vertexToEdgeSep = "->";
	private String edgeSep = ",";
	private Logger logger;
	private static final String LOG_FILE_PREFIX = JPregelConstants.LOG_DIR;
	private static final String LOG_FILE_SUFFIX = ".log";

	private void initLogger() throws IOException {
		this.logger = JPregelLogger.getLogger(this.getId(), LOG_FILE_PREFIX
				+ this.getId() + LOG_FILE_SUFFIX);
	}

	private String getId() {

		return "GraphGenerator";
	}

	public GraphGenerator(int numOfVertices, int maxcost) throws IOException,
			IllegalInputException {
		this.numOfVertices = numOfVertices;
		this.maxcost = maxcost;
		this.initLogger();
		this.generateGraph();
	}

	private void generateGraph() throws IllegalInputException {
		List<Edge> edgeList = new Vector<Edge>();
		int destVertex = -1;
		int cost;
		int numOutgoingEdges = -1;
		Map<Integer, Integer> existingNums = new HashMap<Integer, Integer>();
		StringBuffer strBuff = new StringBuffer();
		Edge newEdge = null;
		final int VALUE = 0;

		if (numOfVertices == 0 || numOfVertices == 1) {
			throw new IllegalInputException(
					"Graph cannot be generated with zero or one vertex!");
		}
		for (int i = 0; i < numOfVertices; i++) {
			existingNums.clear();
			existingNums.put(i, VALUE);
			logger.info("Source vertex id :" + i);
			edgeList.clear();

			if (i != 0) {
				logger.info("Adding an edge to the previous vertex : " + i
						+ "->" + (i - 1));
				cost = new Random().nextInt(this.maxcost);

				newEdge = new Edge(Integer.toString(i),
						Integer.toString(i - 1), cost);
				edgeList.add(newEdge);
				existingNums.put(i - 1, VALUE);
			}
			destVertex = -1;
			
			while (numOutgoingEdges == -1) {
				if (numOfVertices > 3) {
					numOutgoingEdges = new Random().nextInt(numOfVertices / 2);
				} else {
					numOutgoingEdges = new Random().nextInt(numOfVertices -1 );
				}
			}
			logger.info("No of outgoing edges for source " + i + " is "
					+ numOutgoingEdges);
			for (int j = 0; j < numOutgoingEdges; j++) {

				int count = 0;
				destVertex = new Random().nextInt(numOfVertices);

				while (existingNums.containsKey(destVertex) && count < 5) {
					
					destVertex = new Random().nextInt(numOfVertices);
					logger.info("Found dest vertex :" + destVertex);
					count++;
				}
				if(count < 5) {
				existingNums.put(destVertex, VALUE);
				logger.info("Destination vertex is " + destVertex);
				cost = new Random().nextInt(this.maxcost);
				logger.info("Cost of the edge :" + cost);
				newEdge = new Edge(Integer.toString(i),
						Integer.toString(destVertex), cost);
				edgeList.add(newEdge);
				destVertex = -1;

			}
			}
			
			// Vertex newVertex = new Vertex(Integer.toString(i), value,
			// edgeList);
			String edgeString = returnString(edgeList);
			strBuff.append(i + vertexToEdgeSep + edgeString);
			strBuff.append("\n");
			logger.info("Content of strBuffer: " + strBuff + "\n");
			numOutgoingEdges = -1;

		}
		try {
			BufferedWriter writefile = new BufferedWriter(new FileWriter(
					graphFilePath));

			String output = strBuff.toString();
			logger.info("Content in Output after toString :" + output);
			writefile.write(output);
			writefile.close();
		} catch (IOException e) {
			logger.info("Could not open File " + graphFilePath + "for writing");
			e.printStackTrace();
		}

	}

	private String returnString(List<Edge> edgeList) {
		String str = "";

		boolean firstItemCrossed = false;
		for (Edge e : edgeList) {
			if (firstItemCrossed) {
				str += edgeSep;
			}
			firstItemCrossed = true;
			str += e.toString();
		}

		return str;

	}

	public static String getRandomChars() {
		char first = (char) ((new Random().nextInt(26)) + 65);
		char second = (char) ((new Random().nextInt(26)) + 65);
		char third = (char) ((new Random().nextInt(26)) + 65);
		return "" + first + second + third;
	}

	public static void main(String args[]) {

		try {
			try {
				GraphGenerator graph = new GraphGenerator(20, 100);
			} catch (IllegalInputException e) {

				e.printStackTrace();
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

}
