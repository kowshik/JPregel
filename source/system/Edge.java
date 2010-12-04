/**
 * 
 */
package system;

//Abstracts an edge in a graph
public class Edge {
	public String sourceVertexID;
	public static final String vertexToCostSep = ":";
	
	public String getSourceVertexID() {
		return sourceVertexID;
	}

	public void setSourceVertexID(String sourceVertexID) {
		this.sourceVertexID = sourceVertexID;
	}

	public String destVertexID;
	public String getDestVertexID() {
		return destVertexID;
	}

	public void setDestVertexID(String destVertexID) {
		this.destVertexID = destVertexID;
	}

	public int cost;
	
	public int getCost() {
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}

	public Edge(String sourceVertexID, String destVertexID, int cost){
		if(sourceVertexID==null){
			throw new NullPointerException("sourceVertexID = null");
		}
		this.sourceVertexID=sourceVertexID;
		if(destVertexID==null){
			throw new NullPointerException("destVertexID == null");
		}
		this.destVertexID=destVertexID;
		this.cost=cost;
	}
	
	public Edge(String sourceVertexID, String edgeString) throws IllegalInputException{
		if(sourceVertexID==null){
			throw new NullPointerException("sourceVertexID = null");
		}
		this.sourceVertexID=sourceVertexID;
		
		String[] vertexToCost=edgeString.split(vertexToCostSep);
		if(vertexToCost.length!=2){
			throw new IllegalInputException(edgeString);
		}
		this.setDestVertexID(vertexToCost[0]);
		this.setCost(Integer.parseInt(vertexToCost[1]));
	}
	public String toString(){
		return this.getDestVertexID()+vertexToCostSep+this.getCost();
	}
}
