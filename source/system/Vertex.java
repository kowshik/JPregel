package system;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Vertex {

	private String vertexID;
	public static final String vertexToEdgesSep = "->";
	public static final String vertexToVertexSep = ",";
	public static final String vertexToCostSep = ":";
	
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

	private Map<String, Integer> outgoingEdges;

	public Vertex(String vertexID, Integer value,  Map<String, Integer> outgoingEdges) {
		this.vertexID = vertexID;
		this.value = value;
		this.outgoingEdges = outgoingEdges;
		if(outgoingEdges==null){
			this.outgoingEdges=new LinkedHashMap<String, Integer>();
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
		String[] outgoingVertices = vertexToEdges[1].split(vertexToVertexSep);
		this.outgoingEdges=new LinkedHashMap<String, Integer>();
		for (String vertexDetail : outgoingVertices) {
			String[] vertexToCost=vertexDetail.split(vertexToCostSep);
			if(vertexToCost.length!=2){
				throw new IllegalInputException(adjacencyListRecord+" ---   "+vertexToEdges[1]+"  ----  "+vertexDetail);
			}
			String outgoingVertexId=vertexToCost[0];
			
			this.outgoingEdges.put(outgoingVertexId,Integer.parseInt(vertexToCost[1]));
			
		}

	}
	
	
	public String toString(){
		String str=this.getVertexID()+vertexToEdgesSep;
		Set<Map.Entry<String,Integer>> mapEntries = outgoingEdges.entrySet();
		
		boolean firstItemCrossed=false;
		for(Map.Entry<String,Integer> e : mapEntries){
			if(firstItemCrossed){
				str+=vertexToVertexSep;
			}
			firstItemCrossed=true;
			str+=e.getKey();
			str+=vertexToCostSep;
			str+=e.getValue();
		}
		
		return str;
	}
	
/*	public static void main(String[] args) throws IllegalInputException{
		String a="A->B:25,C:35,D:45";
		String b="B->D:34,E:12,F:56";
		
		Vertex va=new Vertex(a);
		Vertex vb=new Vertex(b);
		
		System.err.println(va);
		System.err.println(vb);
		
	}*/
}
