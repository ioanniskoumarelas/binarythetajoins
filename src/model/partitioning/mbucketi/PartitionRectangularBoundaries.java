/**
 * PartitionRectangularBoundaries.java
 * 
 * Secondary structure to aid the PartitionRectangular.
 * 
 * It includes the boundaries of a rectangular region.
 * This is formed by the upper-left and bottom-right 
 * points.
 * 
 * @author John Koumarelas
 */

package model.partitioning.mbucketi;

import datatypes.IntPair;

public class PartitionRectangularBoundaries {
	
	IntPair startUpper;
	IntPair endBottom;

	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("[");
			sb.append("[" + startUpper.getFirst() + "," + startUpper.getSecond() + "]");
				sb.append(",");
			sb.append("[" + endBottom.getFirst() + "," + endBottom.getSecond() + "]");
		sb.append("]");
				
		return sb.toString();
	}
	
	public IntPair getStartUpper() {
		return startUpper;
	}

	public void setStartUpper(IntPair startUpper) {
		this.startUpper = startUpper;
	}

	public IntPair getEndBottom() {
		return endBottom;
	}

	public void setEndBottom(IntPair endBottom) {
		this.endBottom = endBottom;
	}

	public PartitionRectangularBoundaries(){
		startUpper = new IntPair(-1, -1);
		endBottom = new IntPair(-1, -1);
	}
	
	public void setStartBoundaries(int startUpperRow, int startUpperCol){
		this.startUpper.set(startUpperRow, startUpperCol);
	}
	
	public void setEndBoundaries( int endBottomRow, int endBottomCol) {
		this.endBottom.set(endBottomRow, endBottomCol);
	}
	
	public void setEndBoundaries( int endBottomCol) {
		this.endBottom.set(endBottom.getFirst(), endBottomCol);
	}
	
	public int getRows() {
		return getEndBottom().getFirst() - getStartUpper().getFirst()+1;
	}
	
	public int getColumns() {
		return getEndBottom().getSecond() - getStartUpper().getSecond()+1;
	}
	
}
