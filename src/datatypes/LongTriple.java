/**
 * LongTriple.java
 * 
 * Define a triple of long values that are writable. They are serialized in a byte
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

public class LongTriple implements WritableComparable<LongTriple> {
	private long first = 0;
	private long second = 0;
	private long third = 0;
	
	public LongTriple() {
		// TODO Auto-generated constructor stub
	}

	public void set(long first, long second, long third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	public LongTriple(long first, long second, long third) {
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
		first = in.readLong() + Long.MIN_VALUE;
		second = in.readLong() + Long.MIN_VALUE;
		third = in.readLong() + Long.MIN_VALUE;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(first - Long.MIN_VALUE);
		out.writeLong(second - Long.MIN_VALUE);
		out.writeLong(third - Long.MIN_VALUE);
	}

	@Override
	public int hashCode() {
		return (int) (463*first + 157*second + third);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof LongTriple) {
			LongTriple r = (LongTriple) right;
			return r.first == first && r.second == second && r.third == third;
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized IntPair. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(LongTriple.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(LongTriple.class, new Comparator());
	}

	@Override
	public int compareTo(LongTriple o) {
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
	
	public long getFirst() {
		return first;
	}

	public long getSecond() {
		return second;
	}

	public long getThird() {
		return third;
	}
	
}
