/**
 * PartitionRectangular.java
 * 
 * Implements the default concept of the Partition as described at:
 * "Processing theta-joins using MapReduce".
 * 
 * which form only rectangular regions.
 * 
 * @author John Koumarelas
 */

package model.partitioning.mbucketi;

import model.PartitionMatrix;
import model.partitioning.Partition;
import datatypes.IntPair;

public class PartitionRectangular extends Partition {
	
	private PartitionRectangularBoundaries rb;
	
	private long outputCost = -1;
	
	public PartitionRectangular() {
		super();
		rb = new PartitionRectangularBoundaries();
	}
	
	public PartitionRectangular(int id, PartitionMatrix pm) {
		super(id,pm);
		rb = new PartitionRectangularBoundaries();
	}
	
	public PartitionRectangular(int startUpperRow, int startUpperCol, int id, PartitionMatrix pm) {
		super(id,pm);
		rb = new PartitionRectangularBoundaries();
		setStartBoundaries(startUpperRow, startUpperCol);
	}

	public long candidateInputPartitionCost(int row, int row_i, int col_i) {
		boolean columnFound = false;
		
		long inputCost = 0;
		long[][] matrix = pm.getMatrix();
		long[] countsS = pm.getCountsS();
		long[] countsT = pm.getCountsT();
		for(int i = row ; i <= row_i; ++i) {
			if(matrix[i][col_i]>0) {
				if(!candidateS.contains(i)) {
					inputCost += countsS[i];
				}
				if(!candidateT.contains(col_i) && !columnFound) {
					inputCost += countsT[col_i];
					columnFound = true;
				}
			}
		}
		return inputCost;
	}
	
	public void computeOutputCost(long[][] M, long[] countsS, long[] countsT) {
		outputCost = 0;
		
		for(int i = rb.getStartUpper().getFirst() ; i<= rb.getEndBottom().getFirst() ; ++i) {
			for(int j = rb.getStartUpper().getSecond() ; j <= rb.getEndBottom().getSecond(); ++j) {
				if(M[i][j]>0) {
					outputCost += (countsS[i]*countsT[j]);
				}
			}
		}
	}
	
	public long getOutputCost() {
		if(outputCost == -1) {
			System.err.println("ERROR! Output cost given is -1!");
			System.exit(-1);
		}
		return outputCost;
	}
	
	private void setStartBoundaries(int startUpperRow, int startUpperCol) {
		rb.setStartBoundaries(startUpperRow, startUpperCol);
	}
	
	public void addAllCellsWithinRegionBoundaries(long[][] matrix) {
		for(int i = rb.getStartUpper().getFirst() ; i<= rb.getEndBottom().getFirst(); ++i) {
			for(int j = rb.getStartUpper().getSecond(); j <= rb.getEndBottom().getSecond(); ++j){
				if(matrix[i][j] > 0) {
					addCell(i,j);
				}
			}
		}
	}
	
	private void setEndBoundaries(int endRow, int endCol) {
		rb.setEndBoundaries(endRow, endCol);
	}

	public void addColumn(long[][] M, int col_i,int row, int row_i) {
		for(int i = row ; i <= row_i; ++i) {
			if(M[i][col_i]>0) {
				addCell(i,col_i);
			}
		}
		setEndBoundaries(row_i, col_i);
	}
	
	public void addColumns(long[][] M, int colStart, int colEnd, int rowStart, int rowEnd) {
		for(int c = colStart ; c<= colEnd ; ++c){
			addColumn(M,c,rowStart,rowEnd);
		}
	}
	
	public void addRow(long[][] M, int row_i,int col, int col_i) {
		for(int j = col ; j <= col_i; ++j) {
			if(M[row_i][j]>0) {
				addCell(row_i,j);
			}
		}
		setEndBoundaries(row_i, col_i);
	}
	
	public void addRows(long[][] M, int colStart, int colEnd, int rowStart, int rowEnd) {
		for(int r = rowStart ; r<= rowEnd ; ++r){
			addRow(M,r,colStart,colEnd);
		}
	}
	
	@Override
	public String toString() {
		long ic = this.computeInputCost();
		return "id: " + id + " " + rb.toString() + " inputCost: " + ic + "\n" + super.toString();
	}
		
	public IntPair getRbStartUpper() {
		return rb.getStartUpper();
	}
	
	public IntPair getRbEndBottom() {
		return rb.getEndBottom();
	}
	
	public int getColumns() {
		return rb.getColumns();
	}
	
	public int getRows() {
		return rb.getRows();
	}
	
	public String getRbToString() {
		return rb.toString();
	}


}
