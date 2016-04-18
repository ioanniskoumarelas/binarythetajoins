package test.rearranging;

import model.BucketBoundaries;
import model.PartitionMatrix;
import rearranging.BondEnergyAlgorithm;
import datatypes.exceptions.RearrangementError;

public class TestBondEnergyAlgorithm {
	
	// TODO: introduce JUnitTests.
	
	private void printPartitionMatrix(PartitionMatrix pm) {
		long[][] matrix = pm.getMatrix();
		for(int ri = 0 ; ri < matrix.length ; ++ri) {
			for(int ci = 0; ci < matrix[0].length ; ++ci) {
				System.out.print(matrix[ri][ci]);
				if((ci+1)<matrix[0].length){
					System.out.print(",");
				}
			}
			System.out.println("");
		}
	}
	
	private void runTestCaseSmall() throws RearrangementError {
		PartitionMatrix pm = new PartitionMatrix();
		
//		int[][] matrix = {
//				{1,0,1,0,1},
//				{0,1,0,1,0},
//				{1,0,1,0,1},
//				{0,1,0,1,0},
//				{1,0,1,0,1}};
		
		long[][] matrix = {
				{0,0,0,0},
				{Long.MAX_VALUE,0,Long.MAX_VALUE,0},
				{Long.MAX_VALUE,5,Long.MAX_VALUE,0},
				{1,0,Long.MAX_VALUE,0}};
		
		pm.setMatrix(matrix);
		
		long sizeS = 4;
		long sizeT = 4;
		
		int bucketsS = 4;
		int bucketsT = 4;
		
		pm.setSizeS(sizeS);
		pm.setSizeT(sizeT);
		
		BucketBoundaries[] boundariesS = new BucketBoundaries[bucketsS];
		BucketBoundaries[] boundariesT = new BucketBoundaries[bucketsT];
		
		long[] countsS = new long[bucketsS];
		long[] countsT = new long[bucketsT];
		
		for(int i = 0 ; i < bucketsS ; ++i) {
			boundariesS[i] = new BucketBoundaries();
			boundariesS[i].set(i, i+1);
			
			countsS[i] = sizeS/bucketsS;
		}
		for(int i = 0 ; i < bucketsT ; ++i) {
			boundariesT[i] = new BucketBoundaries();
			boundariesT[i].set(i, i+1);
			
			countsT[i] = sizeT/bucketsT;
		}

		pm.setBoundariesS(boundariesS);
		pm.setBoundariesT(boundariesT);
		pm.setCountsS(countsS);
		pm.setCountsT(countsT);
		
		BondEnergyAlgorithm bea = new BondEnergyAlgorithm(pm);
		
		System.out.println("Before rearrangements [BEA]");
		printPartitionMatrix(pm);
		
		bea.rearrange();
		
		System.out.println("After rearrangements [BEA]");
		printPartitionMatrix(pm);
	}
	
	public static void main(String[] args) throws RearrangementError {
		
		TestBondEnergyAlgorithm tbea = new TestBondEnergyAlgorithm();
		tbea.runTestCaseSmall();
		
	}
	
}
