/**
 * PartitionMatrix.java
 * 
 * This class models the partition matrix.
 * 
 * Each cell corresponds to a combination of buckets from S and T.
 * The cell which is located at (i,j), corresponds to the
 * i-th bucket from S
 * j-th bucket from T
 * 
 * Encapsulates all the necessary information to describe the Partition Matrix.
 * 
 * @author John Koumarelas
 */

package model;

public class PartitionMatrix {
	protected long[][] matrix;
	protected BucketBoundaries[] boundariesS;
	protected BucketBoundaries[] boundariesT; // bucket boundary
	protected long[] countsS;
	protected long[] countsT; // bucket count
	
	protected long sizeS; // sum of countsS
	protected long sizeT; // sum of countsT

	/*	SETTERS - GETTERS	*/
	
	public long getSizeS() {
		return sizeS;
	}
	public void setSizeS(long sizeS) {
		this.sizeS = sizeS;
	}
	public long getSizeT() {
		return sizeT;
	}
	public void setSizeT(long sizeT) {
		this.sizeT = sizeT;
	}
	public int getBucketsS(){
		return boundariesS.length;
	}
	public int getBucketsT() {
		return boundariesT.length;
	}
	public long[] getCountsS() {
		return countsS;
	}
	public void setCountsS(long[] countsS) {
		this.countsS = countsS;
	}
	public long[] getCountsT() {
		return countsT;
	}
	public void setCountsT(long[] countsT) {
		this.countsT = countsT;
	}
	public long[][] getMatrix() {
		return matrix;
	}
	public void setMatrix(long[][] matrix) {
		this.matrix = matrix;
	}
	public BucketBoundaries[] getBoundariesS() {
		return boundariesS;
	}
	public void setBoundariesS(BucketBoundaries[] boundariesS) {
		this.boundariesS = boundariesS;
	}
	public BucketBoundaries[] getBoundariesT() {
		return boundariesT;
	}
	public void setBoundariesT(BucketBoundaries[] boundariesT) {
		this.boundariesT = boundariesT;
	}
}
