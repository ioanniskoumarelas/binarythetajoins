/**
 * PartitionMatrixSwapper.java
 * 
 * After the rearrangements took place,
 * this class actually swaps the PartitionMatrix according
 * to the index rearrangements that the previous
 * methods have decided.
 * 
 * @author John Koumarelas
 */

package utils.handlers.partitionMatrix;

import model.BucketBoundaries;
import model.PartitionMatrix;

public class PartitionMatrixSwapper {

	private PartitionMatrix pm;
	
	private long[][] matrix;
	
	private BucketBoundaries[] boundariesS;
	private BucketBoundaries[] boundariesT;
	
	private long[] countsS;
	private long[] countsT;
	
	public PartitionMatrixSwapper(PartitionMatrix pm) {
		this.pm = pm;
		this.matrix = pm.getMatrix();
		this.boundariesS = pm.getBoundariesS();
		this.boundariesT = pm.getBoundariesT();
		this.countsS = pm.getCountsS();
		this.countsT = pm.getCountsT();
	}
	
	public void swap(int[] swapRows, int[] swapColumns) {
		createNewMatrix(swapRows, swapColumns);
		createNewBuckets(swapRows, swapColumns);
		
		pm.setBoundariesS(boundariesS);
		pm.setBoundariesT(boundariesT);
		pm.setCountsS(countsS);
		pm.setCountsT(countsT);
		pm.setMatrix(matrix);
	}
	
	public void swap(int[] swapRows) {
		createNewMatrix(swapRows);
		createNewBuckets(swapRows);
		
		pm.setBoundariesS(boundariesS);
		pm.setBoundariesT(boundariesT);
		pm.setCountsS(countsS);
		pm.setCountsT(countsT);
		pm.setMatrix(matrix);
	}

	private void createNewMatrix(int[] swapRows, int[] swapColumns) {
		long[][] newMatrix = new long[boundariesS.length][boundariesT.length];

		for (int i = 0; i < boundariesS.length; ++i) {
			for (int j = 0; j < boundariesT.length; ++j) {
				newMatrix[i][j] = matrix[swapRows[i]][swapColumns[j]];
			}
		}

		matrix = newMatrix;
	}
	
	private void createNewMatrix(int[] swapRows) {
		long[][] newMatrix = new long[boundariesS.length][boundariesT.length];

		for (int i = 0; i < boundariesS.length; ++i) {
			for (int j = 0; j < boundariesT.length; ++j) {
				newMatrix[i][j] = matrix[swapRows[i]][j];
			}
		}

		matrix = newMatrix;
	}

	private void createNewBuckets(int[] swapRows, int[] swapColumns) {
		BucketBoundaries[] newBoundariesS = new BucketBoundaries[boundariesS.length];
		BucketBoundaries[] newBoundariesT = new BucketBoundaries[boundariesT.length]; // bucket boundary
		long[] newCountsS = new long[countsS.length];
		long[] newCountsT = new long[countsT.length]; // bucket count

		for (int i = 0; i < boundariesS.length; ++i) {
			newBoundariesS[i] = new BucketBoundaries();
			newBoundariesS[i].set(boundariesS[swapRows[i]].getFrom(),boundariesS[swapRows[i]].getTo());
		}

		for (int i = 0; i < countsS.length; ++i) {
			newCountsS[i] = countsS[swapRows[i]];
		}

		for (int j = 0; j < boundariesT.length; ++j) {
			newBoundariesT[j] = new BucketBoundaries();
			newBoundariesT[j].set(boundariesT[swapColumns[j]].getFrom(),boundariesT[swapColumns[j]].getTo());
		}

		for (int j = 0; j < countsT.length; ++j) {
			newCountsT[j] = countsT[swapColumns[j]];
		}

		boundariesS = newBoundariesS;
		countsS = newCountsS;

		boundariesT = newBoundariesT;
		countsT = newCountsT;
	}

	private void createNewBuckets(int[] swapRows) {
		BucketBoundaries[] newBoundariesS = new BucketBoundaries[boundariesS.length];
		long[] newCountsS = new long[countsS.length];

		for (int i = 0; i < boundariesS.length; ++i) {
			newBoundariesS[i] = new BucketBoundaries();
			newBoundariesS[i].set(boundariesS[swapRows[i]].getFrom(), boundariesS[swapRows[i]].getTo());
		}

		for (int i = 0; i < countsS.length; ++i) {
			newCountsS[i] = countsS[swapRows[i]];
		}

		boundariesS = newBoundariesS;
		countsS = newCountsS;
	}

}
