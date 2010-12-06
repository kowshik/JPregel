/**
 * 
 */
package system;

//Abstracts an edge in a graph
public class Edge {
	public int sourceVertexID;
	public static final String vertexToCostSep = ":";

	public int getSourceVertexID() {
		return sourceVertexID;
	}

	public void setSourceVertexID(int sourceVertexID) {
		this.sourceVertexID = sourceVertexID;
	}

	public int destVertexID;

	public int getDestVertexID() {
		return destVertexID;
	}

	public void setDestVertexID(int destVertexID) {
		this.destVertexID = destVertexID;
	}

	public int cost;

	public int getCost() {
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}

	public Edge(int sourceVertexID, int destVertexID, int cost) {

		this.sourceVertexID = sourceVertexID;

		this.destVertexID = destVertexID;
		this.cost = cost;
	}

	public Edge(int sourceVertexID, String edgeString)
			throws IllegalInputException {

		this.sourceVertexID = sourceVertexID;

		String[] vertexToCost = edgeString.split(vertexToCostSep);
		if (vertexToCost.length != 2) {
			throw new IllegalInputException(edgeString);
		}

		int vertexID = -1;

		try {
			vertexID = Integer.parseInt(vertexToCost[0]);

		} catch (NumberFormatException e) {
			throw new IllegalInputException(edgeString);
		}

		if (vertexID < 0) {
			throw new IllegalInputException(edgeString);
		}

		this.setDestVertexID(vertexID);
		this.setCost(Integer.parseInt(vertexToCost[1]));
	}

	public String toString() {
		return this.getDestVertexID() + vertexToCostSep + this.getCost();
	}
}
