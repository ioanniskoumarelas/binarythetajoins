/**
 * TSPAlgorithmTransposed.java
 * 
 * A modification of the default TSPAlgorithm, that tries to 
 * rearrange both the columns and the rows (which the default algorithm
 * rearranges only). It rearranged the rows and after that it uses
 * the default matrix to find rearrangements for the columns. It
 * assumes that rows and columns rearrangements are independent.
 * 
 * @author John Koumarelas
 */
package rearranging;

import java.io.IOException;

import model.PartitionMatrix;
import model.partitioning.mbucketi.PartitionRectangularBoundaries;

import org.apache.hadoop.fs.Path;

import utils.handlers.partitionMatrix.PartitionMatrixSwapper;
import utils.importers.RearrangementsImporter;
import datatypes.exceptions.RearrangementError;


public class TSPAlgorithmTransposedOnDefault extends Rearrangements {

	private PartitionMatrix pm;
	private int numPartitions;
	private int k;
	
	private Path partitionMatrixDirectory;
	
	public TSPAlgorithmTransposedOnDefault(PartitionMatrix pm, int numPartitions, int k, 
			Path partitionMatrixDirectory) {
		this.pm = pm;
		this.numPartitions = numPartitions;
		this.k = k;
		this.partitionMatrixDirectory = partitionMatrixDirectory;
	}

	@Override
	void execute() throws RearrangementError {
		try {
			long[][] originalMatrix = pm.getMatrix().clone();
			
			TSPAlgorithm tspa;
			
			tspa = new TSPAlgorithm(pm,numPartitions,k,"TSPkTransposedOnDefault",partitionMatrixDirectory);
			tspa.rearrange();
			
			int[] rowsMapping = null;
			
			try {
				rowsMapping = TSPAlgorithm.getTSPReorder(new RearrangementsImporter().importTSPkSolution(tspa.getTspkWorkingDirectory()),k);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			this.rowsRearrangements = rowsMapping;
			
			pm.setMatrix(originalMatrix.clone());
			
			/*	Transpose matrix	*/
			long[][] tmpMatrix = new long[pm.getMatrix().length][pm.getMatrix()[0].length];
			
			for(int i = 0 ; i < tmpMatrix.length; ++i) {
				for(int j = 0 ; j < tmpMatrix[0].length ; ++j) {
					tmpMatrix[i][j] = pm.getMatrix()[j][i];
				}
			}
			
			pm.setMatrix(tmpMatrix);
			
			tspa = new TSPAlgorithm(pm,numPartitions,k,"TSPkTransposedOnDefault",partitionMatrixDirectory);
			tspa.rearrange();
			
			int[] columnsMapping = null;
			
			try {
				columnsMapping = TSPAlgorithm.getTSPReorder(new RearrangementsImporter().importTSPkSolution(tspa.getTspkWorkingDirectory()),k);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			this.columnsRearrangements = columnsMapping;
			
			pm.setMatrix(originalMatrix.clone());
			
			PartitionRectangularBoundaries rb = new PartitionRectangularBoundaries();		
			rb.setStartBoundaries(0, 0);
			rb.setEndBoundaries(pm.getBucketsS()-1 , pm.getBucketsT()-1);
			
			new PartitionMatrixSwapper(pm).swap(rowsMapping, columnsMapping);
		} catch(Exception e){
			System.err.println("Rearrangement not happened!");
        	e.printStackTrace();
        	throw new RearrangementError(e.getMessage());
		}
		
	}
}
