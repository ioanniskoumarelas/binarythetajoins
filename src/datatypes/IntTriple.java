/**
 * IntTriple.java
 * 
 * Define a triple of integers that are writable. They are serialized in a byte
 * comparable format.
 * 
 * @author John Koumarelas
 */

package datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntTriple implements WritableComparable<IntTriple> {
	private int first = 0;
	private int second = 0;
	private int third = 0;

	public void set(int first, int second, int third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	/**
	 * Read the two integers. Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE,
	 * MAX_VALUE-> -1
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt() + Integer.MIN_VALUE;
		second = in.readInt() + Integer.MIN_VALUE;
		third = in.readInt() + Integer.MIN_VALUE;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first - Integer.MIN_VALUE);
		out.writeInt(second - Integer.MIN_VALUE);
		out.writeInt(third - Integer.MIN_VALUE);
	}

	@Override
	public int hashCode() {
		return 463*first + 157*second + third;
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof IntTriple) {
			IntTriple r = (IntTriple) right;
			return r.first == first && r.second == second && r.third == third;
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized IntPair. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntTriple.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(IntTriple.class, new Comparator());
	}

	@Override
	public int compareTo(IntTriple o) {
		if (first != o.first) {
			return first < o.first ? -1 : 1;
		} else if (second != o.second) {
			return second < o.second ? -1 : 1;
		} else if (third != o.third) {
			return third < o.third ? -1 : 1;
		} else {
			return 0;
		}
	}
	
	@Override
	public String toString() {
		return "[" + first + ","  + second + "," + third + "]";
	}
	
	/*	Getters - Setters	*/
	
	public int getFirst() {
		return first;
	}

	public int getSecond() {
		return second;
	}

	public int getThird() {
		return third;
	}
}
