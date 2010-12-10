package graphs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import system.JPregelConstants;
import exceptions.IllegalInputException;

/**
 * Generates random graphs to test the system. The graphs generated are
 * moderately connected in a random manner, but never disconnected.
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class GraphGenerator {

	private int numOfVertices;
	private String graphFilePath = JPregelConstants.GRAPH_FILE;
	private int maxcost;
	private String vertexToEdgeSep = "->";
	private String edgeSep = ",";
	
	/**
	 * 
	 * @param numOfVertices
	 *            Number of vertices required in the output graph
	 * @param maxcost
	 *            Maximum possible cost value of each edge in the graph
	 * @throws IOException
	 * @throws IllegalInputException
	 */
	public GraphGenerator(int numOfVertices, int maxcost) throws IOException,
			IllegalInputException {
		this.numOfVertices = numOfVertices;
		this.maxcost = maxcost;

	}

	/**
	 * Generates the graph, and writes it to the location defined by
	 * JPregelConstants.GRAPH_FILE
	 * 
	 * @throws IllegalInputException
	 */
	public void generateGraph() throws IllegalInputException {
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
			// logger.info("Source vertex id :" + i);
			edgeList.clear();

			if (i != 0) {
				// logger.info("Adding an edge to the previous vertex : " + i
				// + "->" + (i - 1));
				cost = new Random().nextInt(this.maxcost);

				newEdge = new Edge(i, i - 1, cost);
				edgeList.add(newEdge);
				existingNums.put(i - 1, VALUE);
			}
			destVertex = -1;

			while (numOutgoingEdges == -1) {
				if (numOfVertices == 2) {
					numOutgoingEdges = 1;
				} else if (numOfVertices == 3) {
					numOutgoingEdges = new Random().nextInt(numOfVertices - 2) + 1;
				} else {
					numOutgoingEdges = new Random().nextInt(numOfVertices / 2) + 1;
				}
			}
			// logger.info("No of outgoing edges for source " + i + " is "
			// + numOutgoingEdges);
			for (int j = 0; j < numOutgoingEdges; j++) {

				int count = 0;
				destVertex = new Random().nextInt(numOfVertices);

				while (existingNums.containsKey(destVertex) && count < 5) {

					destVertex = new Random().nextInt(numOfVertices);
					// logger.info("Found dest vertex :" + destVertex);
					count++;
				}
				if (count < 5) {
					existingNums.put(destVertex, VALUE);
					// logger.info("Destination vertex is " + destVertex);
					cost = new Random().nextInt(this.maxcost);
					// logger.info("Cost of the edge :" + cost);
					newEdge = new Edge(i, destVertex, cost);
					edgeList.add(newEdge);
					destVertex = -1;

				}
			}

			// Vertex newVertex = new Vertex(Integer.toString(i), value,
			// edgeList);
			String edgeString = returnString(edgeList);
			strBuff.append(i + vertexToEdgeSep + edgeString);
			strBuff.append("\n");
			// logger.info("Content of strBuffer: " + strBuff + "\n");
			numOutgoingEdges = -1;

		}
		try {
			BufferedWriter writefile = new BufferedWriter(new FileWriter(
					graphFilePath));

			String output = strBuff.toString();
			// logger.info("Content in Output after toString :" + output);
			writefile.write(output);
			writefile.close();
		} catch (IOException e) {
			// logger.info("Could not open File " + graphFilePath +
			// "for writing");
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

		int numVertices = Integer.parseInt(args[0]);
		int maxCost = Integer.parseInt(args[1]);
		try {
			try {
				GraphGenerator graph = new GraphGenerator(numVertices, maxCost);
				graph.generateGraph();
			} catch (IllegalInputException e) {

				e.printStackTrace();
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

}
