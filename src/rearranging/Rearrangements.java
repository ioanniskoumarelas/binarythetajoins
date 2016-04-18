/**
 * Rearrangements.java
 * 
 * It is an abstract class that represents the rearrangements
 * that will be implemented with different methods. These rearrangements
 * will be saved as a re-mapping of the indices.
 * 
 * Example of use: 
 * 
 * matrix[0][0]: 1
 * matrix[0][1]: 2
 * matrix[1][0]: 3
 * matrix[1][1]: 4
 * 
 * print matrix[0][0] // 1
 * 
 * if rowRearrangements[0]=1, columnRearrangements[0]=0
 * 
 * print matrix[rowRearrangements[0]][columnRearrangements[0]] // 3
 * 
 * @author John Koumarelas
 */

package rearranging;

import model.PartitionMatrix;
import datatypes.exceptions.RearrangementError;

public abstract class Rearrangements {
	
	protected PartitionMatrix pm;
	
	protected int[] rowsRearrangements = null;
	protected int[] columnsRearrangements = null;

	private long executionTime;

	abstract void execute() throws RearrangementError;

	public void rearrange() throws RearrangementError {
		long start = System.currentTimeMillis();
		execute();
		long end = System.currentTimeMillis();
		executionTime =  end-start;
	}
	
	public long getExecutionTime() {
		return executionTime;
	}
	
	public int[] getRowsRearrangements() {
		return rowsRearrangements;
	}
	
	public int[] getColumnsRearrangements() {
		return columnsRearrangements;
	}
	
}