package system;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class Vertex {

	private String vertexID;
	public static final String vertexToEdgesSep = "->";
	public static final String edgesSep = ",";
	
	
	public String getVertexID() {
		return vertexID;
	}

	public void setVertexID(String vertexID) {
		this.vertexID = vertexID;
	}

	private Integer value;
	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	private List<Edge> outgoingEdges;

	public Vertex(String vertexID, Integer value,  List<Edge> outgoingEdges) {
		this.vertexID = vertexID;
		this.value = value;
		this.outgoingEdges = outgoingEdges;
		if(outgoingEdges==null){
			this.outgoingEdges=new Vector<Edge>();
		}
	}

	public Vertex(String adjacencyListRecord) throws IllegalInputException {
		
		// Example : A->B:3,C:5,D:25

		/*
		 * Example : A->B:3,C:5,D:25 is split into
		 * vertexToEdges[0] = A:0
		 * vertexToEdges[1] = B:3,C:5,D:25
		 */
		String[] vertexToEdges = adjacencyListRecord.split(vertexToEdgesSep);
		if (vertexToEdges.length != 2) {
			throw new IllegalInputException(adjacencyListRecord);
		}
		this.setVertexID(vertexToEdges[0]);
		this.setValue(JPregelConstants.INFINITY);
		
		/*
		 * Example : B:3,C:5,D:25 is split into
		 * outgoingVertices[0] = B:3
		 * outgoingVertices[1] = C:5
		 * outgoingVertices[2] = D:25
		 */
		String[] outgoingEdges = vertexToEdges[1].split(edgesSep);
		this.outgoingEdges=new Vector<Edge>();
		for (String edgeDetail : outgoingEdges) {
			Edge e=new Edge(this.getVertexID(), edgeDetail);
			this.outgoingEdges.add(e);
		}

	}
	
	
	public String toString(){
		String str=this.getVertexID()+vertexToEdgesSep;
		
		boolean firstItemCrossed=false;
		for(Edge e : outgoingEdges){
			if(firstItemCrossed){
				str+=edgesSep;
			}
			firstItemCrossed=true;
			str+=e.toString();
		}
		
		return str;
	}
	
	public static void main(String[] args) throws IllegalInputException{
		String a="A->B:25,C:35,D:45,E:34";
		String b="B->D:34,E:12,F:56";
		
		Vertex va=new Vertex(a);
		Vertex vb=new Vertex(b);
		
		System.err.println(va);
		System.err.println(vb);
		
	}
}
