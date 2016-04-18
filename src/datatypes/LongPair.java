/**
 * LongPair.java
 * 
 * 
 * Define a pair of long values that are writable. They are serialized in a byte
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

public class LongPair implements WritableComparable<LongPair> {
	private long first = 0;
	private long second = 0;

	/**
	 * Set the left and right values.
	 */
	public void set(long left, long right) {
		first = left;
		second = right;
	}

	public LongPair(long left, long right) {
		first = left;
		second = right;
	}
	
	public LongPair() {
		first = 0;
		second = 0;
	}
	
	/**
	 * Read the two integers. Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE,
	 * MAX_VALUE-> -1
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readLong() + Long.MIN_VALUE;
		second = in.readLong() + Long.MIN_VALUE;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(first - Long.MIN_VALUE);
		out.writeLong(second - Long.MIN_VALUE);
	}

	@Override
	public int hashCode() {
		return (int) (first * 157 + second);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof LongPair) {
			LongPair r = (LongPair) right;
			return r.first == first && r.second == second;
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized LongPair. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(LongPair.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(LongPair.class, new Comparator());
	}

	@Override
	public int compareTo(LongPair o) {
		if (first != o.first) {
			return first < o.first ? -1 : 1;
		} else if (second != o.second) {
			return second < o.second ? -1 : 1;
		} else {
			return 0;
		}
	}
	
	@Override
	public String toString() {
		return "[" + first + ","  + second + "]";
	}
	
	/*	Getters - Setters */
	
	public long getFirst() {
		return first;
	}

	public long getSecond() {
		return second;
	}
}
