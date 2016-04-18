/**
 * VirtualPartitionMatrixFactory.java
 * 
 * This class is deprecated. Use of it is not recommended.
 * (Modify at your own will of course!)
 * 
 * It is supposed to allow the creation of AD-HOC
 * PartitionMatrices, that does not rely on a specific
 * Dataset to be created, and test mostly visual differences.
 * 
 * @author John Koumarelas
 */

package utils.factories;

import java.util.Arrays;
import java.util.Random;

import model.BucketBoundaries;
import model.PartitionMatrix;
import datatypes.IntPair;

/**
 * @deprecated
 */
public class VirtualPartitionMatrixFactory {
	
	private int buckets;
	private int bands;
	private int bandsOffsetSeed;
	private int sparsity;
	
	private String bandType;
	
	private long sizeS;
	private long sizeT;
	
	private BucketBoundaries[] boundariesS;
	private BucketBoundaries[] boundariesT;
	private long[] countsS;
	private long[] countsT;
	private long[][] matrix;

	private PartitionMatrix pm;
	
	private long executionTime;

	public VirtualPartitionMatrixFactory(int buckets, int sparsity, int bands, int bandsOffsetSeed, String bandType) {
		this.buckets = buckets;
		this.bands = bands;
		this.bandsOffsetSeed = bandsOffsetSeed;
		this.sparsity = sparsity;
		this.bandType = bandType;
		
		boundariesS = new BucketBoundaries[buckets];
		boundariesT = new BucketBoundaries[buckets];
		
		for(int i = 0 ; i < buckets; ++i) {
			boundariesS[i] = new BucketBoundaries();
			boundariesT[i] = new BucketBoundaries();
		}
		
		countsS = new long[buckets];
		countsT = new long[buckets];
		matrix = new long[buckets][buckets];

		for(int i = 0 ; i < buckets; ++i) {
			Arrays.fill(matrix[i],0);
		}
	}

	public PartitionMatrix getPartitionMatrix() {
		long start = System.currentTimeMillis();
		
		PartitionMatrix pm = null;
		if(bandType.equals("normalstep")) {
			pm = caseBandJoinNormalLineMovement(sparsity);
		} else { // if randomstep
			pm = caseBandJoinRandomLineMovement(sparsity);
		}
		long end = System.currentTimeMillis();
		executionTime = end-start;
		return pm;
	}
	
	void setBoundaries(){
		for(int i = 0 ; i < buckets; ++i) {
			boundariesS[i].set(i, i+1);
			boundariesT[i].set(i, i+1);
		}
	}
	
	void calculateCountAndSize() {
		/*	XXX: Calculate countsS and countsT differently.	*/
		/* 		   	this implementation uses the same cell value two times,	*/
		/* 			one for the row and one for the column	*/
		
		for(int i = 0 ; i < buckets; ++i){
			countsS[i]=0;
			for(int j = 0 ; j < buckets; ++j) {
				if(matrix[i][j]>0) {
					countsS[i] = 1;
					break;
				}
			}
		}
		
		for(int j = 0 ; j < buckets; ++j){
			countsT[j]=0;
			for(int i = 0 ; i < buckets; ++i) {
				if(matrix[i][j]>0) {
					countsT[j] = 1;
					break;
				}
			}
		}
		
		sizeS = buckets;
		sizeT = buckets;
	}
	
	void setFieldsPartitionMatrix() {
		pm = new PartitionMatrix();
		
		pm.setMatrix(matrix);
		pm.setBoundariesS(boundariesS);
		pm.setBoundariesT(boundariesT);
		pm.setCountsS(countsS);
		pm.setCountsT(countsT);
		pm.setSizeS(sizeS);
		pm.setSizeT(sizeT);
	}

	public PartitionMatrix caseBandJoinRandomLineMovement(int cellsPerRow) {
		Random random = new Random(bandsOffsetSeed);
	
		if(bands%2==1) // odd
		{
			/*	XXX: WARNING the first value is not random, we should just produce it and ignore it.	*/
			random.nextDouble();
			
			IntPair[] startFrom = new IntPair[bands];
			for (int i = 0 ; i < bands; ++i) {
				startFrom[i] = new IntPair();
			}
			
			int counterBands = 0;
			
			int stepRows = -1;
			
			stepRows= (int) (buckets/(bands/2.0));
			int stepColumns = -1; 
			
			
			stepColumns = (int) (buckets/(bands/2.0));
			
			/*	FIRST POINT	*/
			/*	Start bands at top-right most	*/
			/*	First band	*/
			double rnd = random.nextDouble()*0.2;
			
			if(bands == 1) {
				startFrom[counterBands++].set(0,0);
			} else {
				startFrom[counterBands++].set(0, (int) ((stepColumns*0.5)*(0.8+rnd)));
			}
			
			if (bands > 1) {
				/*	Bands that start at some columns	*/
				for(int j = 0; j < bands / 2; ++j) {
					rnd = random.nextDouble()*0.2;
					
					int row = 0;
					int column = (int) ((j+1)*stepColumns + ((stepColumns/2.0)*(0.8+rnd)));
					
					if(column < 0) {
						column = 0;
					}
					
					startFrom[counterBands++].set(row, column);
				}
				
				/*	Bands that start at some rows	*/
				for(int i = 0 ; i< bands/2; ++i) {
					rnd = random.nextDouble()*0.2;
					
					int row = (int) ((i+1)*stepRows - ((stepRows/2.0)*(0.8+rnd)));
					
					if(row < 0) {
						row = 0;
					}
					
					int column = 0;
					
					startFrom[counterBands++].set(row, column);
				}
			}
			
			
			/*	now all bands start from the correct point	*/
			for(int k = 0 ; k < bands; ++k) {
				
				int previousRow = -1;
				int curRow = startFrom[k].getFirst();
				int curCol = startFrom[k].getSecond();
				
				while(curRow < buckets && curCol < buckets) {
					if(previousRow != -1) {
						int i = previousRow;
						for(i = previousRow; i < curRow ; ++i) {
//							for(int j = curCol - cellsPerRow/2 ; j < curCol + cellsPerRow/2; ++j)
								matrix[i][curCol] = 1;
//								matrix[i][j] = 1;
						}
						int j;
						for(i = previousRow; i < curRow ; ++i) {
							j = curCol - cellsPerRow/2;
							if(j<0) {
								j = 0;
							}
							for(;j < buckets && j < curCol + cellsPerRow/2; ++j) {
//								matrix[i][curCol] = 1;
								matrix[i][j] = 1;
							}
						}
					}
					matrix[curRow][curCol] = 1;
					
					rnd = random.nextDouble();
					
					if(rnd < 0.5) { // the band will step down (on rows)
						rnd = random.nextDouble()*0.1*stepRows;
						if(rnd < 1.0) {
							rnd = 1.0;
						}
						
						previousRow = curRow;
						curRow += rnd;
						++curCol;
					} else { // the band will not step down | row will not change
						++curCol;
					}
				}
			}
		} else { // even
			random.nextDouble();
			
			IntPair[] startFrom = new IntPair[bands];
			for(int i = 0 ; i < bands; ++i) {
				startFrom[i] = new IntPair();
			}
			
			int counterBands = 0;
			
			int stepRows = -1;
			
			stepRows= (int) (buckets/(bands/2.0));
			int stepColumns = -1; 
			
			
			stepColumns = (int) (buckets/(bands/2.0));
			
			/*	FIRST POINT	*/
			/*	Start bands at top-right most	*/
			/*	First band	*/
			double rnd = random.nextDouble()*0.2;
			
			/*	Bands that start at some columns	*/
			for(int j = 0 ; j<bands/2; ++j) {
				rnd = random.nextDouble()*0.2;
				
				int row = 0;
				int column = (int) (j*stepColumns + ((stepColumns/2.0)*(0.8+rnd)));
				
				if(column < 0) {
					column = 0;
				}
				
				startFrom[counterBands++].set(row, column);
			}
			
			/*	Bands that start at some rows	*/
			for(int i = 0 ; i< bands/2; ++i) {
				rnd = random.nextDouble()*0.2;
				
				int row = (int) (i*stepRows - ((stepRows/2.0)*(0.8+rnd)));
				
				if(row < 0) {
					row = 0;
				}
				
				int column = 0;
				
				startFrom[counterBands++].set(row, column);
			}
			
			
			/*	now all bands start from the correct point	*/
			for(int k = 0 ; k < bands; ++k) {
				
				int previousRow = -1;
				int curRow = startFrom[k].getFirst();
				int curCol = startFrom[k].getSecond();
				
				while (curRow < buckets && curCol < buckets) {
					if (previousRow != -1) {
						int i = previousRow;
						for (i = previousRow; i < curRow ; ++i) {
//							for(int j = curCol - cellsPerRow/2 ; j < curCol + cellsPerRow/2; ++j)
								matrix[i][curCol] = 1;
//								matrix[i][j] = 1;
						}
						int j;
						for (i = previousRow; i < curRow ; ++i) {
							j = curCol - cellsPerRow / 2;
							if (j < 0)
								j = 0;
							for(; j < buckets && j < curCol + cellsPerRow/2; ++j)
//								matrix[i][curCol] = 1;
								matrix[i][j] = 1;
						}
					}
					matrix[curRow][curCol] = 1;
					
					rnd = random.nextDouble();
					
					if (rnd < 0.5) { // the band will step down (on rows)
						rnd = random.nextDouble()*0.1*stepRows;
						if (rnd < 1.0)
							rnd = 1.0;
						
						previousRow = curRow;
						curRow += rnd;
						++curCol;
					} else { // the band will not step down | row will not change
						++curCol;
					}
				}
			}
		}
		
		setBoundaries(); // set some default boundariesS and boundariesT.
		calculateCountAndSize(); // FIXME: check how count and size are calculated.
		setFieldsPartitionMatrix(); // create PartitionMatrix and set fields.
	
		return pm;
	}
	
	public PartitionMatrix caseBandJoinNormalLineMovement(int cellsPerRow) {
		Random random = new Random(bandsOffsetSeed);

		if(bands%2==1) { // odd
		
			/*	XXX: WARNING the first value is not random, we should just produce it and ignore it.	*/
			random.nextDouble();
			
			IntPair[] startFrom = new IntPair[bands];
			for (int i = 0 ; i < bands; ++i) {
				startFrom[i] = new IntPair();
			}
			
			int counterBands = 0;
			
			int stepRows = (int) (buckets/((double)bands/2));
			int stepColumns = (int) (buckets/((double)bands/2));
			
			double rndSelRowOrCol = random.nextDouble();
			if (rndSelRowOrCol > 0.5) { // Play with the row
				double rnd = random.nextDouble()*0.5;
				
				startFrom[counterBands++].set((int) (stepRows*rnd), 0);
			} else { // play with the column
				double rnd = random.nextDouble()*0.5;
				
				startFrom[counterBands++].set(0, (int) (stepColumns*rnd));
			}
			
			if (bands > 1) {
				for (int i = 0 ; i< bands/2; ++i) {
					double rnd = random.nextDouble() - 0.5;
					
					int row = (int) ((i+1)*stepRows + rnd*stepRows);
					
					if (row < 0) {
						row = 0;
					}
					
					int column = 0;
					
					startFrom[counterBands++].set(row, column);
				}
				
				for (int j = 0 ; j<bands/2; ++j) {
					double rnd = random.nextDouble() - 0.5;
					
					int row = 0;
					int column = (int) ((j+1)*stepColumns + rnd*stepColumns);
					
					if (column < 0) {
						column = 0;
					}
					
					startFrom[counterBands++].set(row, column);
					
				}
			}
			
			for (int k = 0; k < bands; ++k) {
				int curCol = startFrom[k].getSecond();
				for (int i = startFrom[k].getFirst(); i < buckets; ++i) {
					
					matrix[i][curCol] = 1;
					
					int col = curCol - cellsPerRow/2;
					if (col < 0) {
						col = 0;
					}
					
					int counterCol = 0;
					
					for (;counterCol < cellsPerRow && col < buckets; ++counterCol,++col) {
						matrix[i][col] = 1;
					}

					++curCol;
					if (curCol >= buckets) {
						break;
//						curCol = bucketsT-1;
					}
				}
			}
		} else { // even
			/*	XXX: WARNING the first value is not random, we should just produce it and ignore it.	*/
			random.nextDouble();
			
			IntPair[] startFrom = new IntPair[bands];
			for(int i = 0 ; i < bands; ++i) {
				startFrom[i] = new IntPair();
			}
			
			int counterBands = 0;
			
			int stepRows = (int) (buckets/(bands/2.0));
			int stepColumns = (int) (buckets/(bands/2.0));
			
			/*	Bands that start at column 0. (Row bands)	*/
			for(int i = 0 ; i< bands/2; ++i) {
				double rnd = random.nextDouble() - 0.5;
				
				int row = (int) ((i+1)*stepRows/2.0 + rnd*stepRows);
				
				if (row < 0) {
					row = 0;
				}
				
				int column = 0;
				
				startFrom[counterBands++].set(row, column);
			}
			
			/*	Bands that start at row 0. (Column bands)	*/
			for (int j = 0 ; j < bands / 2; ++j) {
				double rnd = random.nextDouble() - 0.5;
				
				int row = 0;
				int column = (int) ((j+1)*stepColumns/2.0 + rnd*stepColumns);
				
				if (column < 0) {
					column = 0;
				}
				
				startFrom[counterBands++].set(row, column);
				
			}
			
			for (int k = 0 ; k < bands; ++k) {
				int curCol = startFrom[k].getSecond();
				for (int i = startFrom[k].getFirst(); i < buckets; ++i) {
					
					matrix[i][curCol] = 1;
					
					int col = curCol - cellsPerRow/2;
					if (col < 0) {
						col = 0;
					}
					
					int counterCol = 0;
					
					for (; counterCol < cellsPerRow && col < buckets; ++counterCol,++col) {
						matrix[i][col] = 1;
					}

					++curCol;
					if (curCol >= buckets) {
						break;
					}
				}
			}
		}
		
		setBoundaries(); // set some default boundariesS and boundariesT.
		calculateCountAndSize(); // FIXME: check how count and size are calculated.
		setFieldsPartitionMatrix(); // create PartitionMatrix and set fields.
	
		return pm;
	}
	
	public long getExecutionTime() {
		return executionTime;
	}
	
}
