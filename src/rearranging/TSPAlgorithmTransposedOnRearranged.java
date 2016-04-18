/** TSPAlgorithmTransposedOnRearranged.java
 * 
 * 
 * A modification of the default TSPAlgorithm, that tries to 
 * rearrange both the columns and the rows (which the default algorithm
 * rearranges only). It rearranged the rows and on the rearranged matrix
 * it tries to find rearrangements for the columns as well. It assumes
 * rows and columns rearrangements are dependent.
 * 
 * @author John Koumarelas
 */
package rearranging;

import java.io.IOException;

import model.PartitionMatrix;

import org.apache.hadoop.fs.Path;

import utils.importers.RearrangementsImporter;
import datatypes.exceptions.RearrangementError;

/**
 * @deprecated
 */
public class TSPAlgorithmTransposedOnRearranged extends Rearrangements {

	private PartitionMatrix pm;
	private int numPartitions;
	private int k;
	
	private Path partitionMatrixDirectory;
	
	public TSPAlgorithmTransposedOnRearranged(PartitionMatrix pm, int numReducers, int k, 
			Path partitionMatrixDirectory) {
		this.pm = pm;
		this.numPartitions = numPartitions;
		this.k = k;
		this.partitionMatrixDirectory = partitionMatrixDirectory;
	}

	@Override
	void execute() throws RearrangementError {
		try {
			TSPAlgorithm tspa;
			
			tspa = new TSPAlgorithm(pm,numPartitions,k,"TSPkTransposedOnRearranged",partitionMatrixDirectory);
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

			/*	Transpose matrix	*/
			long[][] tmpMatrix = new long[pm.getMatrix().length][pm.getMatrix()[0].length];
			
			for(int i = 0 ; i < tmpMatrix.length; ++i) {
				for(int j = 0 ; j < tmpMatrix[0].length ; ++j) {
					tmpMatrix[i][j] = pm.getMatrix()[j][i];
				}
			}
			
			tspa = new TSPAlgorithm(pm,numPartitions,k, "TSPkTransposedOnRearranged",partitionMatrixDirectory);
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
		} catch(Exception e) {
			System.err.println("Rearrangement not happened!");
        	e.printStackTrace();
        	throw new RearrangementError(e.getMessage());
		}
	}
}
