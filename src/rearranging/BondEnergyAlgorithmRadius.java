/**
 * BondEnergyAlgorithmRadius.java
 * 
 * A slight modification of the BondEnergyAlgorithm,
 * in which we take into account a range of cells to compute
 * the bond energy.
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

public class BondEnergyAlgorithmRadius extends Rearrangements {
	
	private long[][] M;
    private List<Set<Integer>> Mrows;
    private List<Set<Integer>> Mcols;
	private int[] placedRows;
	private int placedRowsCounter;
	private int[] placedColumns;
	private int placedColumnsCounter;
	
	private int radius;
	
	private long maxBond;
	private int maxPositionToInsert;

	public BondEnergyAlgorithmRadius(PartitionMatrix pm, int radius) {
		this.pm = pm;
		this.radius = radius;
	}

	private long computeRadiusBond(List<Set<Integer>> Mhash, int c, int lower, int upper,boolean rows, boolean outterLower) {
		long bond = 0;
		
		// radius == 1+
        if(Mhash.get(lower).contains(c)){            
            if(rows) {
            	bond += (long)M[upper][c]*M[lower][c];
            } else {
            	bond += (long)M[c][upper]*M[c][lower];
            }
        }
        
        // radius > 1
        if(radius > 1) {
        	for(int i = 1 ; i < radius; ++i) {
        		
        		// left/up
        		if((c-i) >= 0) {
        			if(rows) {
                    	bond += (long)M[upper][c-i]*M[lower][c-i];
                    } else {
                    	bond += (long)M[c-i][upper]*M[c-i][lower];
                    }
        		}
        		
        		// right/down
        		if((c+i) < (rows ? M.length : M[0].length)) {
        			if(rows) {
                    	bond += (long)M[upper][c+i]*M[lower][c+i];
                    } else {
                    	bond += (long)M[c+i][upper]*M[c+i][lower];
                    }
        		}
        	}
        }
        
        return bond;
	}
	
	private long computeBond(List<Set<Integer>> Mhash, int upper, int lower, boolean rows) {
		long bond = 0;
		if(Mhash.get(upper).size() <= Mhash.get(lower).size()){
            for(int c : Mhash.get(upper)){
            	bond += computeRadiusBond(Mhash,c,upper,lower,rows,rows);
            }
        } else {
            for(int c : Mhash.get(lower)){
            	bond += computeRadiusBond(Mhash,c,upper,lower,rows,rows);
            }
        }
		
		bond *= 2;
		return bond;
	}
	
	private long getBond(List<Set<Integer>> Mhash, int[] placed, int placedCounter, 
			int cIndexM, int cIndexPlaced, long totalBond, boolean rows){
		
		long bond = 0;
		
		bond += totalBond;
		
		if(cIndexPlaced > 0 && cIndexPlaced < placedCounter) {
			bond -= computeBond(Mhash,placed[cIndexPlaced],placed[cIndexPlaced+1],rows);
			
			bond += computeBond(Mhash,placed[cIndexPlaced],cIndexM,rows);
			bond += computeBond(Mhash,cIndexM,placed[cIndexPlaced+1],rows);
		} else if(cIndexPlaced == 0) {
			bond += computeBond(Mhash,cIndexM,placed[0],rows);
		} else if(cIndexPlaced == placedCounter) {
			bond += computeBond(Mhash,cIndexM,placed[placedCounter-1],rows);
		} 
			
		return bond;
	}
        
	// @rows: true: if you are searching at the rows, false: if you are searching at the columns
	private void findMaxBond(List<Set<Integer>> Mhash, int[] placed, int placedCounter, int candidateIndex, boolean rows) {
		long bond = 0;

		maxBond = -1;
		maxPositionToInsert = -1;
		
		// Calculate the whole bond
		long totalBond = 0;
		for(int i = 0 ; i < placedCounter - 1; ++i)
			totalBond += computeBond(Mhash,placed[i],placed[i+1],rows);
		
		for(int i = 0 ; i <= placedCounter ; ++i) {

			bond = getBond(Mhash,placed,placedCounter,candidateIndex,i, totalBond,rows);
			
			if(bond > maxBond) {
				maxBond = bond;
				maxPositionToInsert = i;
			}
		}
	}
	
	private void placeRow(int row,int positionToInsert) {
		// Ideal case. Insert at the bottom of the array
		if(positionToInsert == placedRowsCounter) {
			placedRows[positionToInsert] = row;
		}
		else { // Shift rows down and insert at maxPositionToInsert
			for(int i = placedRowsCounter; i > positionToInsert; --i) {
				placedRows[i] = placedRows[i-1];
			}
			placedRows[positionToInsert] = row;
		}
		++placedRowsCounter;
	}
	
	
	private void placeColumn(int column,int positionToInsert) {
		// Ideal case. Insert at the right (bottom) of the array
		if(positionToInsert == placedColumnsCounter) {
			placedColumns[positionToInsert] = column;
		}
		else { // Shift columns right and insert at maxPositionToInsert
			for(int j = placedColumnsCounter; j > positionToInsert; --j) {
				placedColumns[j] = placedColumns[j-1];
			}
			placedColumns[positionToInsert] = column;
		}
		++placedColumnsCounter;
	}
	
    @Override
	protected void execute() throws RearrangementError {
		try {
			M = pm.getMatrix();
	        getMrowsHash();
	        getMcolsHash();
			
			int rows = pm.getBoundariesS().length;
			int columns = pm.getBoundariesT().length;
			
			boolean continueLoop = true;
			
			boolean[] placedRowFlag = new boolean[rows];
			for(int i = 0 ; i < rows; ++i)
				placedRowFlag[i] = false;
			
			boolean[] placedColumnFlag = new boolean[columns];
			for(int j = 0 ; j < columns; ++j)
				placedColumnFlag[j] = false;
			
			placedRows = new int[rows]; placedRowsCounter = 0;
			placedColumns = new int[columns]; placedColumnsCounter = 0;
			
			long totalMaxBond=-1; // max value of bond
			int totalMaxCandRow=-1; // max candidate row to be placed
			int totalMaxRowPositionToInsert=-1; // position of the max candidate row to be placed
			int totalMaxCandColumn=-1; // max candidate column to be placed
			int totalMaxColumnPositionToInsert=-1; // position of the max candidate column to be placed
			
			int i=0,j=0;
			int step = 1;
			
			while(continueLoop) {
				switch(step) {
					case 1:
						for(int ri = 0 ; ri < rows; ++ri)
							if(placedRowFlag[ri] == false) {
								++i;
								placedRowFlag[ri] = true;
								placedRows[placedRowsCounter++] = ri;
								break;
							}
						step = 2;
						break;
					case 2:
						totalMaxBond = totalMaxCandRow = totalMaxRowPositionToInsert = -1;
						
						for(int ri = 0 ; ri < rows; ++ri){
							if(placedRowFlag[ri] == false) {
								findMaxBond(Mrows, placedRows, placedRowsCounter, ri,true);
								if(maxBond > totalMaxBond) {
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
						if(i<rows)
							step = 2;
						else
							step = 4;
						break;
					case 4:
						for(int ci = 0 ; ci < columns; ++ci)
							if(placedColumnFlag[ci] == false) {
								++j;
								placedColumnFlag[ci] = true;
								placedColumns[placedColumnsCounter++] = ci;
								break;
							}
						step = 5;
						break;
					case 5:
						totalMaxBond = totalMaxCandColumn = totalMaxColumnPositionToInsert = -1;
						
						for(int ci = 0 ; ci < columns; ++ci){
							if(placedColumnFlag[ci] == false) {
								findMaxBond(Mcols,placedColumns, placedColumnsCounter,ci,false);
								if(maxBond > totalMaxBond) {
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
						if(j<columns)
							step = 5;
						else
							continueLoop = false;
						break;
				}
			}
			
			new PartitionMatrixSwapper(pm).swap(placedRows, placedColumns);
			this.rowsRearrangements = placedRows;
			this.columnsRearrangements = placedColumns;
		} catch(Exception e) {
			System.err.println("Rearrangement not happened!");
        	e.printStackTrace();
			throw new RearrangementError(e.getMessage());
		}
	}
	
    private void getMrowsHash() {
        Mrows = new ArrayList<Set<Integer>>(M.length);
        for(long[] row : M){
            HashSet<Integer> colsHash = new HashSet<Integer>();
            for(int c=0; c <= row.length-1; c++){
            	if (row[c] > 0){
                    colsHash.add(c);
                }
            }
            Mrows.add(colsHash);
        }
    }

    private void getMcolsHash() {
        Mcols = new ArrayList<Set<Integer>>(M.length);
        for(int c=0; c < M[0].length; c++){
        	HashSet<Integer> rowsHash = new HashSet<Integer>();
        	for(int r=0; r < M.length; r++){
        		if (M[r][c] > 0){
                    rowsHash.add(r);
                }
        	}
        	 Mcols.add(rowsHash);
        }  
    }
}
