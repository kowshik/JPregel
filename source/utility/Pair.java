/**
 * 
 */
package utility;

import java.io.Serializable;


/**
 * 
 * Abstracts a pair of objects
 * 
 * @author Manasa Chandrasekhar
 * @author Kowshik Prakasam
 * 
 */
public class Pair<A,B> implements Serializable {

	private static final long serialVersionUID = 1685795769196233024L;
	private A o1;
	private B o2;

	public Pair(A o1, B o2) {
		this.o1 = o1;
		this.o2 = o2;
	}

	
	

	public A getFirst() {
		return o1;
	}

	public B getSecond() {
		return o2;
	}

	public void setFirst(A o) {
		o1 = o;
	}

	public void setSecond(B o) {
		o2 = o;
	}

	@Override
	public boolean equals(Object obj) {
	
		Pair p=(Pair)obj;
		return p.o1.equals(this.o1) && p.o2.equals(this.o2);
	}

	
	@Override
	public String toString() {
		return "Pair{" + o1 + ", " + o2 + "}";
	}

	
	
	

}