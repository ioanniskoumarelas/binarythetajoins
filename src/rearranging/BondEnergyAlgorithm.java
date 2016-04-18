/**
 * BondEnergyAlgorithm.java
 * 
 * Performs rearrangements using the Bond Energy Algorithm.
 * 
 * This algorithm is mainly used in industry to bring closer
 * machines to the parts that need them.
 * 
 * Some useful presentation about the implementation of this
 * algorithm can be found at:
 * https://louisville.edu/speed/faculty/sheragu/Book/Slides/Chapter_6_Heragu.ppt
 * 
 * @author John Koumarelas
 */
package rearranging;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import model.PartitionMatrix;
import utils.handlers.partitionMatrix.PartitionMatrixSwapper;
import datatypes.exceptions.RearrangementError;

public class BondEnergyAlgorithm extends Rearrangements {
	
	private long[][] M; // main matrix
    private List<Set<Integer>> Mrows;
    private List<Set<Integer>> Mcols;
	private int[] placedRows;
	private int placedRowsCounter;
	private int[] placedColumns;
	private int placedColumnsCounter;
	
	private long maxBond;
	private int maxPositionToInsert;
	
	/** 
	 * Computes the Bond Energy value, considering the bond formula.
	 * 
	 * @param Mhash: Matrix in a Hash form. Each row/column that has a non-zero value, has an index inserted in the Hash.
	 * @param upper: Upper row/column index.
	 * @param lower: Lower row/column index.
	 * @param rows: If we are checking rows or columns. (TRUE: rows, FALSE: columns)
	 * @return
	 */
	private long computeBond(List<Set<Integer>> Mhash, int upper, int lower, boolean rows) {
		long bond = 0;
		
		Set<Integer> smallerMhash, biggerMhash;
		
		/*	Optimization check: Always iterate through the smaller HashSet values and	*/
		/*	check whether the same row/column index exists in the bigger HashSet.	*/
		if (Mhash.get(upper).size() <= Mhash.get(lower).size()){
			smallerMhash = Mhash.get(upper);
			biggerMhash = Mhash.get(lower);
		} else {
			smallerMhash = Mhash.get(lower);
			biggerMhash = Mhash.get(upper);
		}
		
        for (int c : smallerMhash){
            if (biggerMhash.contains(c)){
                if (rows) {
                	bond += (long)M[upper][c]*M[lower][c];
                } else {
                	bond += (long)M[c][upper]*M[c][lower];
                }
            }
        }
		
        /*	Multiply by 2, because it is supposed to be checked from both sides (upper,lower).	*/
		bond *= 2;
		return bond;
	}
	
	/**
	 * This method checks which of the three placing cases (MIDDLE,TOP,BOTTOM), we are examining. 
	 * MIDDLE: Means that we are going to compute the bond including the upper and lower row/column.
	 * TOP: We consider the row/column as placed at the top of the placed array, to compute the Bond Energy.
	 * BOTTOM: We consider the row/column as placed at the bottom of the placed array, to compute the Bond Energy.
	 * 
	 * @param Mhash: Matrix in a Hash form. Each row/column that has a non-zero value, has an index inserted in the Hash.
	 * @param placed: Array with placed rows. The secondary array that supports the Bond Energy Algorithm.
	 * @param placedCounter: The index of the last placed row/column in the placed array.
	 * @param cIndexM: The row/column index of the matrix which is going to be placed.
	 * @param cIndexPlaced: The row/column index of the placed array, that is currently examined using the Bond formula.
	 * @param totalBond: The total calculated bond value of the placed array.
	 * @param rows: If we are checking rows or columns. (TRUE: rows, FALSE: columns)
	 * @return
	 */
	private long getBond(List<Set<Integer>> Mhash, int[] placed, int placedCounter, 
			int cIndexM, int cIndexPlaced, long totalBond, boolean rows){
		
		long bond = totalBond;
		
		/*	The row/column is going to be checked as a MIDDLE row/column case.	*/ 
		if (cIndexPlaced > 0 && cIndexPlaced < placedCounter) {
			/*	--> We have already calculated the total bond of the placed array (totalBond).	*/ 
			/*	--> The upper and lower row/column of the row that is going to be examined are called neighbors.	*/ 
			/*	--> To compute the new totalBond, we are going to remove the bond value of the neighbors, recalculate	*/
			/*	the bond value of the neighbors of the new examined row/column and sum it to the placed array's total bond.	*/
			bond -= computeBond(Mhash,placed[cIndexPlaced],placed[cIndexPlaced+1],rows);
			
			bond += computeBond(Mhash,placed[cIndexPlaced],cIndexM,rows);
			bond += computeBond(Mhash,cIndexM,placed[cIndexPlaced+1],rows);
		} else if (cIndexPlaced == 0) { // The row/column is going to be checked as the TOP row/column case.
			bond += computeBond(Mhash,cIndexM,placed[0],rows);
		} else if (cIndexPlaced == placedCounter) { // The row/column is going to be checked as the BOTTOM row/column case.
			bond += computeBond(Mhash,cIndexM,placed[placedCounter-1],rows);
		} 
			
		return bond;
	}
        
	/**
	 * 	@param rows: true: if you are searching at the rows, false: if you are searching at the columns	
	 */
	private void findMaxBond(List<Set<Integer>> Mhash, int[] placed, int placedCounter, int candidateIndex, boolean rows) {
		long bond = 0;

		maxBond = Long.MIN_VALUE;
		maxPositionToInsert = Integer.MIN_VALUE;
		
		// Calculate the whole bond
		long totalBond = 0;
		for (int i = 0; i < placedCounter - 1; ++i) {
			totalBond += computeBond(Mhash,placed[i],placed[i+1],rows);
		}
		
		for (int i = 0; i <= placedCounter; ++i) {
			bond = getBond(Mhash,placed,placedCounter,candidateIndex,i, totalBond,rows);
			
			if (bond > maxBond) {
				maxBond = bond;
				maxPositionToInsert = i;
			}
		}
	}
	
	private void placeRow(int row,int positionToInsert) {
		/*	Ideal case. Insert at the bottom of the array	*/
		if(positionToInsert == placedRowsCounter) {
			placedRows[positionToInsert] = row;
		} else { // Shift rows down and insert at maxPositionToInsert
			for(int i = placedRowsCounter; i > positionToInsert; --i) {
				placedRows[i] = placedRows[i-1];
			}
			placedRows[positionToInsert] = row;
		}
		++placedRowsCounter;
	}
	
	
	private void placeColumn(int column,int positionToInsert) {
		/*	Ideal case. Insert at the right (bottom) of the array	*/
		if (positionToInsert == placedColumnsCounter) {
			placedColumns[positionToInsert] = column;
		} else { // Shift columns right and insert at maxPositionToInsert
			for (int j = placedColumnsCounter; j > positionToInsert; --j) {
				placedColumns[j] = placedColumns[j-1];
			}
			placedColumns[positionToInsert] = column;
		}
		++placedColumnsCounter;
	}
	
    @Override
	protected void execute() throws RearrangementError{
        try
        {
        	M = pm.getMatrix();
            getMrowsHash();
            getMcolsHash();

    		int rows = pm.getBoundariesS().length;
    		int columns = pm.getBoundariesT().length;
    		
    		boolean continueLoop = true;
    		
    		boolean[] placedRowFlag = new boolean[rows];
    		for (int i = 0; i < rows; ++i) {
    			placedRowFlag[i] = false;
    		}
    		
    		boolean[] placedColumnFlag = new boolean[columns];
    		for (int j = 0; j < columns; ++j) {
    			placedColumnFlag[j] = false;
    		}
    		
    		placedRows = new int[rows]; placedRowsCounter = 0;
    		placedColumns = new int[columns]; placedColumnsCounter = 0;
    		
    		long totalMaxBond=Long.MIN_VALUE; // max value of bond
    		int totalMaxCandRow=Integer.MIN_VALUE; // max candidate row to be placed
    		int totalMaxRowPositionToInsert=Integer.MIN_VALUE; // position of the max candidate row to be placed
    		int totalMaxCandColumn=Integer.MIN_VALUE; // max candidate column to be placed
    		int totalMaxColumnPositionToInsert=Integer.MIN_VALUE; // position of the max candidate column to be placed
    		
    		int i=0,j=0;
    		int step = 1;

    		while (continueLoop) {
    			switch (step) {
				case 1:
					for (int ri = 0; ri < rows; ++ri)
						if (placedRowFlag[ri] == false) {
							++i;
							placedRowFlag[ri] = true;
							placedRows[placedRowsCounter++] = ri;
							break;
						}
					step = 2;
					break;
				case 2:
					totalMaxBond = Long.MIN_VALUE;
					totalMaxCandRow = totalMaxRowPositionToInsert = Integer.MIN_VALUE;
					
					for (int ri = 0; ri < rows; ++ri){
						if (placedRowFlag[ri] == false) {
							findMaxBond(Mrows, placedRows, placedRowsCounter, ri,true);
							if (maxBond > totalMaxBond) {
								totalMaxBond = maxBond;
								totalMaxCandRow = ri;
								totalMaxRowPositionToInsert = maxPositionToInsert;
							}
						}
					}
			
					placedRowFlag[totalMaxCandRow] = true;
					placeRow(totalMaxCandRow, totalMaxRowPositionToInsert);
					
					step = 3;
					break;
				case 3:
					++i;
					if (i < rows)
						step = 2;
					else
						step = 4;
					break;
				case 4:
					for (int ci = 0; ci < columns; ++ci)
						if (placedColumnFlag[ci] == false) {
							++j;
							placedColumnFlag[ci] = true;
							placedColumns[placedColumnsCounter++] = ci;
							break;
						}
					step = 5;
					break;
				case 5:
					totalMaxBond = Long.MIN_VALUE;
					totalMaxCandColumn = totalMaxColumnPositionToInsert = Integer.MIN_VALUE;
					
					for (int ci = 0; ci < columns; ++ci){
						if (placedColumnFlag[ci] == false) {
							findMaxBond(Mcols,placedColumns, placedColumnsCounter,ci,false);
							if (maxBond > totalMaxBond) {
								totalMaxBond = maxBond;
								totalMaxCandColumn = ci;
								totalMaxColumnPositionToInsert = maxPositionToInsert;
							}
						}
					}
					
					placedColumnFlag[totalMaxCandColumn] = true;
					placeColumn(totalMaxCandColumn, totalMaxColumnPositionToInsert);
					step = 6;
					break;
				case 6:
					++j;
					if (j < columns) {
						step = 5;
					} else {
						continueLoop = false;
					}
					break;
    			}
    		}

    		new PartitionMatrixSwapper(pm).swap(placedRows, placedColumns);
    		this.rowsRearrangements = placedRows;
    		this.columnsRearrangements = placedColumns;
        } catch (Exception e){
        	System.err.println("ERROR - Rearrangements failed");
        	e.printStackTrace();
        	throw new RearrangementError(e.getMessage());
        }
	}

    private void getMrowsHash() {
        Mrows = new ArrayList<Set<Integer>>(M.length);
        for (long[] row : M){
            HashSet<Integer> colsHash = new HashSet<Integer>();
            for (int c = 0; c <= row.length - 1; c++){
            	if (row[c] > 0){
                    colsHash.add(c);
                }
            }
            Mrows.add(colsHash);
        }
    }

    private void getMcolsHash() {
        Mcols = new ArrayList<Set<Integer>>(M.length);
        for(int c = 0; c < M[0].length; c++){
        	HashSet<Integer> rowsHash = new HashSet<Integer>();
        	for (int r = 0; r < M.length; r++){
        		if (M[r][c] > 0){
                    rowsHash.add(r);
                }
        	}
        	 Mcols.add(rowsHash);
        }  
    }
    
    public BondEnergyAlgorithm(PartitionMatrix pm) {
		this.pm = pm;
	}
}