/**
 * BucketBoundaries.java
 * 
 * It implements the LongPair with a semantic behind its name.
 * 
 * It is considered to be used with memory procedures only and 
 * not with frameworks like Hadoop, this is why it does not implement the
 * WritableComparable interface. (This is done to reduce overheads)
 * 
 * @author John Koumarelas
 */

package model;

import org.apache.commons.lang.UnhandledException;

/**
 * Define a pair of integers that are writable. They are serialized in a byte
 * comparable format.
 */
public class BucketBoundaries implements Comparable<BucketBoundaries> {
	private long from = 0;
	private long to;
	/**
	 * Set the left and right values.
	 */
	public void set(long from, long to) {
		this.from = from;
		this.to = to;
	}

	public long getFrom() {
		return from;
	}

	public long getTo() {
		return to;
	}

	@Override
	public int hashCode() {
		return (int) (from * 157 + to);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof BucketBoundaries) {
			BucketBoundaries r = (BucketBoundaries) right;
			return r.from == from && r.to == to;
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(BucketBoundaries other) {
		if (this.from == other.from && this.to == other.to) {
			return 0;
		} else if (this.to < other.from) {
			return -1;
		} else if (this.from > other.to) {
			return 1;
		} else {
			throw new UnhandledException("BucketBoundaries unhandled case happened",
										 new Throwable("BucketBoundaries unhandled case happened"));
		}
	}
	
	@Override
	public String toString() {
		return from + ","  + to;
	}
}
