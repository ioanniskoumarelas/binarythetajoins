/**
 * MBIPartitioner.java
 * 
 * This is the default partitioner as shown at the work:
 * Processing Theta-Joins using MapReduce
 * 
 * @author John Koumarelas
 */
package partitioning.mbucketi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;

import model.PartitionMatrix;
import model.partitioning.Partition;
import model.partitioning.mbucketi.PartitionRectangular;
import partitioning.Partitioner;
import utils.metrics.AllMetrics;
import utils.metrics.ReplicationRateInputCostMetric;
import datatypes.DoubleQuad;
import datatypes.IntPair;
import datatypes.exceptions.PartitioningError;
import datatypes.partitioning.MetricsScore;

public class MBIPartitioner extends Partitioner {
	private int curP;

	private PartitionRectangular[] partitionsR = null;
	private int partitionsCount = 0;
		
	public MBIPartitioner(PartitionMatrix pm, String partitioningPolicy,
			long sizeS, long sizeT, int numPartitions) {
		
		super(pm,partitioningPolicy,sizeS,sizeT,numPartitions, BinarySearchPolicy.MAX_PARTITION_INPUT);
		this.partitionsR = new PartitionRectangular[this.numPartitions];
		this.partitionsCount = 0;
	}
	
	public void findLowest() throws PartitioningError {
		
		this.maxPartitionInput = -1;
		this.curP = numPartitions;
		this.partitionsR = new PartitionRectangular[this.numPartitions];
		this.partitionsCount = 0;
		
		long numCandidateCells = calculateCandidateCells(matrix);
		long lowerBound = (long) (2 * Math.sqrt((double) numCandidateCells / numPartitions));
		long upperBound = sizeS + sizeT;

		long start = System.currentTimeMillis();
		long maxPartitionInput = binarySearchMaxInput(lowerBound, upperBound);
		long end = System.currentTimeMillis();
		executionTimeBinarySearch = end - start;

		if (maxPartitionInput == 0) {
			System.err.println("WARNING - MBIPartitioner.findMaxPartitionInputBinarySearch, max partition input == 0!");
		}
		
		for (int i = 0; i < partitionsCount; ++i){
			partitionsR[i].setId(i);
		}
		idxToPartitionsS = new HashMap<Integer,HashSet<Integer>>();
		idxToPartitionsT = new HashMap<Integer,HashSet<Integer>>();
		calculateIdxToPartitions(partitionsR);

		this.maxPartitionInput = maxPartitionInput; 
	}
	
	@Override
	public void rangeSearch(long lowerMPI, long upperMPI,
			int granularity) throws PartitioningError {
		if (lowerMPI == upperMPI) {
			return;
		}
		
		this.rangeSearchMetrics = new Vector<MetricsScore>();
		
		this.maxPartitionInput = -1;
		this.curP = numPartitions;
		this.partitionsR = new PartitionRectangular[this.numPartitions];
		this.partitionsCount = 0;
		
		long start = System.currentTimeMillis();
		
		long step = (upperMPI - lowerMPI) / granularity;
		
		HashSet<Long> selectedMPIs = new HashSet<Long>();
		long tmpMPI = upperMPI;
		for (int i = 0 ; i < granularity - 1 ; ++i) {
			selectedMPIs.add(tmpMPI);
			tmpMPI -= step;
		}
		selectedMPIs.add(lowerMPI);
		
		for (long mpi : selectedMPIs) {
			maxPartitionInput = mpi;
			boolean result = mBucketI(maxPartitionInput);
			if (!result) {
				break;
			}
			
			Partition[] partitions = this.getPartitions();
			
			int bucketsS = this.countsS.length;
			int bucketsT = this.countsT.length;
			HashMap<String,Double> metrics = new AllMetrics(partitions, this.getPartitionsCount(), 
					sizeS, sizeT,bucketsS, bucketsT, countsS).getAllMetrics();
			
			double score = Double.NEGATIVE_INFINITY;
			
			rangeSearchMetrics.addElement(new MetricsScore(metrics,score));
		}
		
		long end = System.currentTimeMillis();

		executionTimeBinarySearch = end - start;
	}
	
	@Override
	public void execute(long mpi) throws PartitioningError {
		this.maxPartitionInput = -1;
		this.curP = numPartitions;
		this.partitionsR = new PartitionRectangular[this.numPartitions];
		this.partitionsCount = 0;
		
		maxPartitionInput = mpi;
		boolean result = mBucketI(maxPartitionInput);
		
		for (int i = 0; i < partitionsCount; ++i){
			partitionsR[i].setId(i);
		}
		idxToPartitionsS = new HashMap<Integer,HashSet<Integer>>();
		idxToPartitionsT = new HashMap<Integer,HashSet<Integer>>();
		calculateIdxToPartitions(partitionsR);
	}
	
	private int totalCandidateArea(int row, int row_i) {
		
		int nColumns = matrix[0].length;
		
		int counter = 0;
		for (int ri = row; ri <= row_i; ++ri) {
			for (int ci = 0 ; ci < nColumns; ++ci) {
				if (matrix[ri][ci] > 0) {
					++counter;
				}
			}
		}
		return counter;
	}

	private List<PartitionRectangular> coverRows(int row_s, int row_si, long maxPartitionInput) throws PartitioningError {
		List<PartitionRectangular> regions = new ArrayList<PartitionRectangular>();

		PartitionRectangular r = new PartitionRectangular(row_s, 0, -1, pm);

		long curInputSize = 0;

		for (int ci = 0; ci < countsT.length; ++ci) {
			long cap = maxPartitionInput - curInputSize;

			long candInCost = -1;

			candInCost = r.candidateInputPartitionCost(row_s, row_si, ci);
			
			// If there is no available space and the last region has actually at least one candidate cell
			if (cap < candInCost && r.getCandidateCells().size() > 0) {

				r.computeOutputCost(matrix, countsS, countsT);
				regions.add(r);
				r = new PartitionRectangular(row_s, ci, -1, pm);

				curInputSize = 0;

				candInCost = r.candidateInputPartitionCost(row_s, row_si, ci);
				
				r.addColumn(matrix, ci, row_s, row_si);
			} else {
				candInCost = r.candidateInputPartitionCost(row_s, row_si, ci);
				
				r.addColumn(matrix, ci, row_s, row_si);
			}

			curInputSize += candInCost;
		}
		
		if( regions.size() > 0 && r.getCandidateCells().size() == 0) {
			PartitionRectangular rLast = regions.get(regions.size() - 1);
			
			IntPair endBottom = rLast.getRbEndBottom();
			
			for(int ci = endBottom.getSecond() + 1; ci < countsT.length; ++ci) {
				rLast.addColumn(matrix, ci, row_s, row_si);
			}
			
		} else {
			r.computeOutputCost(matrix, countsS, countsT);
			regions.add(r);
		}

//		if(maxPartitionInput < curInputSize) {
//			System.err.println("ERROR: region assigned more cells than it should!");
//			System.exit(0);
//		}

		return regions;
	}

	private int coverSubMatrix(int row_s, long maxPartitionInput) throws PartitioningError {
		double maxScore = -1.0;
		int pUsed = 0;

		int bestRow = -1;

		outerLoop:for (int i = 0; row_s + i < countsS.length; ++i) {
			List<PartitionRectangular> Pi = coverRows(row_s, row_s + i, maxPartitionInput);
			int PiCount = Pi.size();
			
			if( PiCount == 0) {
				System.out.println("Error! No partitions were formed at coverRows! PiCount == 0.");
//				System.exit(0);
			}
			
			for(PartitionRectangular pr : Pi) {
				if(pr.computeInputCost() > maxPartitionInput) {
					break outerLoop; // keep the current best match
				}
			}
			
			int area = totalCandidateArea(row_s, row_s + i);
			double score = Integer.MIN_VALUE;

			score = (double) area / PiCount;

			if (score >= maxScore) {
				maxScore = score;
				bestRow = row_s + i;
				pUsed = PiCount;
			}
		}
		
		if(pUsed == 0) {
			System.out.println("Error! No partitions were formed at coverRows! pUsed == 0");
//			System.exit(0);
		}
		
		curP = curP - pUsed;

		if (curP > numPartitions) {
			String errorMessage = "ERROR - MBIPartitioner.coverSubMatrix, curP > p";
			throw new PartitioningError(errorMessage);
		}
		
		// Assign IDs to partitions. 
		if (maxScore != -1 && curP >= 0) {
			List<PartitionRectangular> Pi = coverRows(row_s, bestRow, maxPartitionInput);

			for (int k = 0; k < Pi.size(); ++k) {
				Pi.get(k).setId(partitionsCount);
				partitionsR[partitionsCount] = Pi.get(k);
				++partitionsCount;
			}
		}
		
		if( bestRow != -1) {
			return bestRow + 1;
		} else {
			return -1;
		}
		
	}

	private void mBucketIClean() {
		for (int i = 0; i < partitionsCount; ++i) {
			partitionsR[i] = null;
		}
		partitionsCount = 0;
		curP = numPartitions;
	}

	// This function implements the Algorithm 3 : M-Bucket-I
	private boolean mBucketI(long maxPartitionInput) throws PartitioningError {
		mBucketIClean();

		int row = 0;
		while (row < countsS.length) {
			row = coverSubMatrix(row, maxPartitionInput);
			if (curP < 0) { // more regions than p are needed.
				return false;
			}
			if (row == -1) { // No regions could be formed (probably cause of a too small max partition input)
				return false;
			} else if (row > countsS.length) {
				String errorMessage = "Row index out of bounds.";
				throw new PartitioningError(errorMessage);
			}
		}
		return true;
	}

	private long binarySearchMaxInput(long lowerBound, long upperBound)
			throws PartitioningError {

		if (lowerBound > upperBound) {
			String errorMessage = 	"ERROR - MBIPartitioner.binarySearchMaxInput," +
									"When searching for the right max input. LowerBound is bigger than UpperBound." +
									"(" + lowerBound + ">" + upperBound + ")" + " No max input returned a true value.";
			throw new PartitioningError(errorMessage);
		}

		/*	calculate midpoint to cut set in half	*/
		long mid = (lowerBound + upperBound) / 2;

		maxPartitionInput = mid;

		boolean result = mBucketI(maxPartitionInput);

		if (!result) {
			return binarySearchMaxInput(mid + 1, upperBound);
		} else {

			if (lowerBound == mid) {
				return mid;
			}

			return binarySearchMaxInput(lowerBound, mid);
		}
	}

	/*	GETTERS - SETTERS	*/
	@Override
	public PartitionRectangular[] getPartitions() {
		return partitionsR;
	}
	@Override
	public Integer getPartitionsCount() {
		return partitionsCount;
	}
}