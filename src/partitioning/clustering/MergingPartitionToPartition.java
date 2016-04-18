/**
 * MergingPartitionToPartition.java
 * 
 * This new partitioner logic tries to produce non-rectangular partitions. 
 * 
 * Variations of this method are based on the metric-measure that is used
 * in order to calculate the distance with which the partitions are formed.
 * 
 * There are several ways with which the merging of the several cells to
 * partitions can be done.
 * 
 * @author John Koumarelas
 */

package partitioning.clustering;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

import datatypes.IntPair;
import datatypes.exceptions.PartitioningError;
import datatypes.partitioning.MetricsScore;
import model.PartitionMatrix;
import model.partitioning.Partition;
import model.partitioning.clustering.PartitionNonRectangular;
import partitioning.Partitioner;
import utils.metrics.AllMetrics;

public class MergingPartitionToPartition extends Partitioner {
	private PartitionNonRectangular[] partitions = null;
	private int partitionsCount = Integer.MIN_VALUE;
	
	private long memoryCounter;
	private long maxMemory;
	/*
	 * Initial
	 */
	
	private double[][] distancePairsInitial = null;
	private TreeMap<Double,HashSet<Integer>> distancesTotalInitial = null;
	private PartitionNonRectangular[] partitionsInitial = null;
	private int partitionsCountInitial = Integer.MIN_VALUE;
	
	/*
	 * Upper triangular matrix
	 */
 
	private double[][] distancePairs = null;
	
	/*
	 * For all partitions we store their distances from other partitions.
	 * In this way we have the minimum distance first.
	 * 	n*(n-1)/2
	 */	
	private TreeMap<Double,HashSet<Integer>> distancesTotal = null;
	
	private boolean[] activePartition = null;
	
	private DistanceMeasure dm = null;
	private HashMap<String, String> dmParameters = null;
	
	public enum DistanceMeasure {
	    JACCARD_BUCKETS("JACCARD_BUCKETS"), MANHATTAN("MANHATTAN"),
	    EMPTIEST_PARTITION_INPUT_COST("EMPTIEST_PARTITION_INPUT_COST"), EMPTIEST_PARTITION_CANDIDATE_CELLS("EMPTIEST_PARTITION_CANDIDATE_CELLS"), 
	    ADDED_INPUT_COST_PARTITION_MAX("ADDED_INPUT_COST_PARTITION_MAX"), ADDED_CANDIDATE_CELLS_PARTITION_MAX("ADDED_CANDIDATE_CELLS_PARTITION_MAX"), 
	    ADDED_INPUT_COST_PARTITION_SUM("ADDED_INPUT_COST_PARTITION_SUM"),
	    WEIGHTED_INPUT_COST_ROWS_COLUMNS("WEIGHTED_INPUT_COST_ROWS_COLUMNS");
	    
	    String measureName;
	    
	    private DistanceMeasure(String measureName) {
			this.measureName = measureName;
		}
	    
	    @Override
	    public String toString() {
	    	return measureName;
	    }
	}
	
	public MergingPartitionToPartition(PartitionMatrix pm, String partitioningPolicy, long sizeS, long sizeT,
										int numPartitions, DistanceMeasure dm, HashMap<String, String> dmParameters, 
										BinarySearchPolicy bsp) {
		super(pm,partitioningPolicy,sizeS,sizeT,numPartitions, bsp);
		this.partitionsCount = 0;
		this.dm = dm;
		this.dmParameters = dmParameters;
		this.memoryCounter = 0L;
		this.maxMemory = Runtime.getRuntime().maxMemory();
	}

	public void findLowest() throws PartitioningError {
		numCandidateCells = calculateCandidateCells(matrix);

		initialize(dm);
		calculateInitialPartitionsDistances(dm);
		
		long lowerBound = Long.MIN_VALUE;
		long upperBound = Long.MIN_VALUE;
		
		long start = System.currentTimeMillis();
		
		switch(bsp) {
			case MAX_PARTITION_INPUT:
				lowerBound = 1L;
				upperBound = sizeS + sizeT;
				maxPartitionInput = binarySearch(dm, lowerBound, upperBound);
				break;
			case MAX_PARTITION_CANDIDATE_CELLS:
				lowerBound = numCandidateCells / numPartitions;
				upperBound = numCandidateCells;
				maxPartitionCandidateCells = binarySearch(dm, lowerBound, upperBound);
				break;
		}
		

		long end = System.currentTimeMillis();
		executionTimeBinarySearch = end - start;
		
		transformToFinal();
		
		/*	Used for debugging	*/
		//printElementDistances();
		
		idxToPartitionsS = new HashMap<Integer,HashSet<Integer>>();
		idxToPartitionsT = new HashMap<Integer,HashSet<Integer>>();

		calculateIdxToPartitions(partitions);
		
		if (maxPartitionInput == 0) {
			System.out.println("Warning, max reducer input == 0!");
		}
	}

	@Override
	public void rangeSearch(long lower, long upper,
			int granularity) throws PartitioningError {
		if (lower == upper) {
			return;
		}
		
		this.rangeSearchMetrics = new Vector<MetricsScore>();
		
		numCandidateCells = calculateCandidateCells(matrix);
				
		initialize(dm);
		calculateInitialPartitionsDistances(dm);
		
		long start = System.currentTimeMillis();
		
		long step = (upper - lower) / granularity;
		
		HashSet<Long> selected = new HashSet<Long>();
		long tmp = upper;
		for (int i = 0 ; i < granularity - 1 ; ++i) {
			selected.add(tmp);
			tmp -= step;
		}
		selected.add(lower);
		
		for (long value : selected) {
			cloneInitialPartitionsDistances();
			switch(bsp){
				case MAX_PARTITION_INPUT:
					maxPartitionInput = value;
					break;
				case MAX_PARTITION_CANDIDATE_CELLS:
					maxPartitionCandidateCells = value;
					break;
			}
			
			boolean result = processMerging(dm);
			if (!result) {
				break;
			}
			//
			
			Partition[] partitions = this.getPartitions();
			
			int bucketsS = this.countsS.length;
			int bucketsT = this.countsT.length;
			HashMap<String,Double> metrics = new AllMetrics(partitions, this.getPartitionsCount(), 
					sizeS, sizeT,bucketsS, bucketsT, countsS).getAllMetrics();
			
			double score = Double.NEGATIVE_INFINITY;
			
			rangeSearchMetrics.addElement(new MetricsScore(metrics,score));
			
//			// XXX: Remove
//			if (mpi > lower && (mpi - step) < lower) {
//				mpi = lower + step;
//			}
		}
		long end = System.currentTimeMillis();
		executionTimeRangeSearch = end - start;
	}

	@Override
	public void execute(long max) throws PartitioningError {
		distancePairs = new double[numCandidateCells][numCandidateCells];
		partitions = new PartitionNonRectangular[numCandidateCells];
		
		cloneInitialPartitionsDistances();
		switch(bsp){
			case MAX_PARTITION_INPUT:
				maxPartitionInput = max;
				break;
			case MAX_PARTITION_CANDIDATE_CELLS:
				maxPartitionCandidateCells = max;
				break;
		}
		
		boolean result = processMerging(dm);
		
		transformToFinal();
		
		/*	Used for debugging	*/
		//printElementDistances();
		
		idxToPartitionsS = new HashMap<Integer,HashSet<Integer>>();
		idxToPartitionsT = new HashMap<Integer,HashSet<Integer>>();

		calculateIdxToPartitions(partitions);
	}

	private void transformToFinal() {
		HashMap<Integer,Integer> initialToFinalID = new HashMap<Integer,Integer>();
		int counter = 0;
		PartitionNonRectangular[] tmpPartitions = new PartitionNonRectangular[partitionsCount]; 
		for (int i = 0 ; i < numCandidateCells; ++i) {
			if (!activePartition[i]){
				continue;
			}
			partitions[i].setId(counter);
			tmpPartitions[counter] = partitions[i];
			initialToFinalID.put(i, counter);
			++counter;
		}
		partitions = tmpPartitions;
		
		double[][] tmpDistancePairs = new double[partitionsCount][partitionsCount];
		for (int i = 0 ; i < numCandidateCells; ++i) {
			if (!activePartition[i]) {
				continue;
			}
			for(int j = i+1 ; j < numCandidateCells; ++j) {
				if (!activePartition[j]) {
					continue;
				}
				tmpDistancePairs[initialToFinalID.get(i)][initialToFinalID.get(j)] = distancePairs[i][j];
			}
		}
		distancePairs = tmpDistancePairs;
	}
	
	public void initialize(DistanceMeasure dm) {
		int numCandidateCells = calculateCandidateCells(matrix); // TODO: store it and don't calculate it each time.
		
		this.maxPartitionInput = Long.MIN_VALUE;
		this.partitionsInitial = new PartitionNonRectangular[numCandidateCells];
		this.partitionsCountInitial = numCandidateCells;
		
		int nRows = matrix.length;
		int nColumns = matrix[0].length;
				
		int counter = 0;
		for (int row = 0; row < nRows; ++row) {
			for (int column = 0 ; column < nColumns; ++column) {
				if (matrix[row][column] > 0){
					PartitionNonRectangular cg = new PartitionNonRectangular(counter,pm);
					
					cg.addCell(row, column);
					
					partitionsInitial[counter] = cg;
					
					++counter;
				}
			}
		}
		
		distancesTotalInitial = new TreeMap<Double,HashSet<Integer>>();
		distancePairsInitial = new double[numCandidateCells][numCandidateCells];
		
		distancesTotal = new TreeMap<Double,HashSet<Integer>>();
		distancePairs = new double[numCandidateCells][numCandidateCells];
		
		for(int i = 0 ; i < numCandidateCells ; ++i) {
			Arrays.fill(distancePairsInitial[i], Double.MIN_VALUE);
			Arrays.fill(distancePairs[i], Double.MIN_VALUE);
		}
		
		activePartition = new boolean[numCandidateCells];
		
		partitions = new PartitionNonRectangular[numCandidateCells];
	}

	private void calculateInitialPartitionsDistances(DistanceMeasure dm) {
		for (int i = 0 ; i < numCandidateCells; ++i) {
			PartitionNonRectangular partition1 = partitionsInitial[i];
			for (int j = i+1 ; j < numCandidateCells; ++j) {
				PartitionNonRectangular partition2 = partitionsInitial[j];
				
				double distance = calculatePartitionsDistance(dm, i, j,true);
				
				distancePairsInitial[i][j] = distance;
				
				/*	Store the distance in a way that it is only stored once per pair of cells.	*/
				/*	Force ordering.	*/
				//Integer pair = new IntPair(partition1.getId(),partition2.getId());
				Integer pair = partition1.getId()*partitionsCountInitial + partition2.getId();
				
				HashSet<Integer> pairs = distancesTotalInitial.get(distance);
				if (pairs == null) {
					pairs = new HashSet<Integer>();
				}
				pairs.add(pair);
				distancesTotalInitial.put(distance,pairs);
				
				/* Freeing memory after some loops --- Increase the step accordingly	*/
				if (++memoryCounter == 50) {
					long freeMemory = Runtime.getRuntime().freeMemory();
					if ((double) freeMemory / maxMemory <= 0.2) {
						System.runFinalization();
						System.gc();
					}
					memoryCounter = 0;
				}
			}
		}
	}
		
	private void cloneInitialPartitionsDistances() {
		for (int i = 0 ; i < numCandidateCells ; ++i) {
			for (int j = i+1 ; j < numCandidateCells ; ++j) {
				distancePairs[i][j] = distancePairsInitial[i][j];
			}
		}
		
		distancesTotal.clear();
		for (Entry<Double,HashSet<Integer>> entryGlobal : distancesTotalInitial.entrySet()){
			HashSet<Integer> partitionPairsNew = new HashSet<Integer>();
			partitionPairsNew.addAll(entryGlobal.getValue());
			
			distancesTotal.put(entryGlobal.getKey(), partitionPairsNew);
		}
		
		Arrays.fill(partitions, null);
		for (int i = 0 ; i < numCandidateCells; ++i) {
			PartitionNonRectangular partitionNR = new PartitionNonRectangular(partitionsInitial[i].getId(),partitionsInitial[i].getPm());
			partitionNR.addPartition(partitionsInitial[i]);
			partitions[i] = partitionNR;
		}
		
		partitionsCount = partitionsCountInitial;
		
		Arrays.fill(activePartition, true);
	}
	/*
	 * Variables that are used only for the calculatePartitionDistance method.
	 */
	private PartitionNonRectangular calcPartition1, calcPartition2;
	private long calcSumInputCostBefore, calcMaxInputCostBefore, calcInputCostAfter;
	private HashSet<Integer> calcCandidateS = new HashSet<Integer>(), calcCandidateT = new HashSet<Integer>();
	private HashSet<IntPair> calcCandidatePairs = new HashSet<IntPair>();
	
	private double calculatePartitionsDistance(DistanceMeasure dm, int partitionID1, int partitionID2, boolean initial) {
		
		calcPartition1 = (initial?partitionsInitial:partitions)[partitionID1];
		calcPartition2 = (initial?partitionsInitial:partitions)[partitionID2];
		switch(dm){
		case JACCARD_BUCKETS:
			calcCandidateS.clear();
			calcCandidateS.addAll(calcPartition1.getCandidateS());
			calcCandidateS.retainAll(calcPartition2.getCandidateS());
			int intersectionS = calcCandidateS.size();
			calcCandidateS.clear();
			calcCandidateS.addAll(calcPartition1.getCandidateS());
			calcCandidateS.addAll(calcPartition2.getCandidateS());
			int unionS = calcCandidateS.size();
			
			calcCandidateT.clear();
			calcCandidateT.addAll(calcPartition1.getCandidateT());
			calcCandidateT.retainAll(calcPartition2.getCandidateT());
			int intersectionT = calcCandidateT.size();
			calcCandidateT.clear();
			calcCandidateT.addAll(calcPartition1.getCandidateT());
			calcCandidateT.addAll(calcPartition2.getCandidateT());
			int unionT = calcCandidateT.size();
			
			double jaccard_similarity = (intersectionS + intersectionT) / (unionS + unionT);
			return 1.0 - jaccard_similarity;
		case MANHATTAN:
			double manhattanDistanceTotal = 0;
			for (IntPair ip1 : calcPartition1.getCandidateCells()) {
				double manhattanDistanceCell = 0.0;
				for (IntPair ip2 : calcPartition2.getCandidateCells()) {
					manhattanDistanceCell += Math.abs(ip1.getFirst() - ip2.getFirst()) + Math.abs(ip1.getSecond() - ip2.getSecond());
				}
				double manhattanDistanceCellAvg = manhattanDistanceCell/calcPartition2.getCandidateCells().size();
				manhattanDistanceTotal += manhattanDistanceCellAvg;
			}
			double manhattanDistanceTotalAvg = manhattanDistanceTotal / calcPartition1.getCandidateCells().size();
			return manhattanDistanceTotalAvg;
		case ADDED_INPUT_COST_PARTITION_MAX:
			calcMaxInputCostBefore = (calcPartition1.computeInputCost() < calcPartition2.computeInputCost() ? calcPartition2.computeInputCost() : calcPartition1.computeInputCost());
			
			calcCandidateS.clear();
			calcCandidateT.clear();
			calcCandidateS.addAll(calcPartition1.getCandidateS());
			calcCandidateS.addAll(calcPartition2.getCandidateS());
			calcCandidateT.addAll(calcPartition1.getCandidateT());
			calcCandidateT.addAll(calcPartition2.getCandidateT());
			
			calcInputCostAfter = 0L;
			
			for (Integer idxS : calcCandidateS) {
				calcInputCostAfter += countsS[idxS];
			}
			for (Integer idxT : calcCandidateT) {
				calcInputCostAfter += countsT[idxT];
			}
			
			return (double) (calcInputCostAfter - calcMaxInputCostBefore);
		case ADDED_CANDIDATE_CELLS_PARTITION_MAX:
			calcMaxInputCostBefore = (calcPartition1.getCandidateCells().size() < calcPartition2.getCandidateCells().size() ? calcPartition2.getCandidateCells().size() : calcPartition1.getCandidateCells().size());
			
			calcCandidatePairs.clear();
			calcCandidatePairs.addAll(calcPartition1.getCandidateCells());
			calcCandidatePairs.addAll(calcPartition2.getCandidateCells());
			
			calcInputCostAfter = calcCandidatePairs.size();
			
			return (double) (calcInputCostAfter - calcMaxInputCostBefore);
		case ADDED_INPUT_COST_PARTITION_SUM:
			calcSumInputCostBefore = calcPartition1.computeInputCost() + calcPartition2.computeInputCost();
			
			calcCandidateS.clear();
			calcCandidateT.clear();
			calcCandidateS.addAll(calcPartition1.getCandidateS());
			calcCandidateS.addAll(calcPartition2.getCandidateS());
			calcCandidateT.addAll(calcPartition1.getCandidateT());
			calcCandidateT.addAll(calcPartition2.getCandidateT());
			
			calcInputCostAfter = 0L;
			
			for (Integer idxS : calcCandidateS) {
				calcInputCostAfter += countsS[idxS];
			}
			for (Integer idxT : calcCandidateT) {
				calcInputCostAfter += countsT[idxT];
			}
			
			/*	XXX (alternative): return (double) -1*(calcSumInputCostBefore - calcInputCostAfter);	*/
			return (double) calcInputCostAfter/calcSumInputCostBefore;
		case EMPTIEST_PARTITION_INPUT_COST:
			calcCandidateS.clear();
			calcCandidateT.clear();
			calcCandidateS.addAll(calcPartition1.getCandidateS());
			calcCandidateS.addAll(calcPartition2.getCandidateS());
			calcCandidateT.addAll(calcPartition1.getCandidateT());
			calcCandidateT.addAll(calcPartition2.getCandidateT());
			
			calcInputCostAfter = 0L;
			
			for (Integer idxS : calcCandidateS) {
				calcInputCostAfter += countsS[idxS];
			}
			for (Integer idxT : calcCandidateT) {
				calcInputCostAfter += countsT[idxT];
			}
			
			if (calcInputCostAfter > maxPartitionInput) {
				return Double.MAX_VALUE;
			} else {
				double partition1InputCost = calcPartition1.computeInputCost();
				double partition2InputCost = calcPartition2.computeInputCost();
				return partition1InputCost < partition2InputCost ? partition1InputCost : partition2InputCost;
			}
		case EMPTIEST_PARTITION_CANDIDATE_CELLS:
			calcCandidatePairs.clear();
			calcCandidatePairs.addAll(calcPartition1.getCandidateCells());
			calcCandidatePairs.addAll(calcPartition2.getCandidateCells());
			
			calcInputCostAfter = calcCandidatePairs.size();
			
			if (calcInputCostAfter > maxPartitionCandidateCells) {
				return Double.MAX_VALUE;
			} else {
				double partition1CandidateCells = calcPartition1.getCandidateCells().size();
				double partition2CandidateCells = calcPartition2.getCandidateCells().size();
				return partition1CandidateCells < partition2CandidateCells ? partition1CandidateCells : partition2CandidateCells;
			}
		case WEIGHTED_INPUT_COST_ROWS_COLUMNS:
			double weightRows = Double.valueOf(dmParameters.get("wR"));
			double weightColumns = Double.valueOf(dmParameters.get("wC"));
			
			if (initial) {
				IntPair ip1 = calcPartition1.getCandidateCells().toArray(new IntPair[0])[0];
				IntPair ip2 = calcPartition2.getCandidateCells().toArray(new IntPair[0])[0];
				
				double weightedDistance = 	weightRows * (countsS[ip1.getFirst()] + countsS[ip2.getFirst()]) +
											weightColumns * (countsT[ip1.getSecond()] + countsT[ip2.getSecond()]);
				
				if (ip1.getFirst() == ip2.getFirst()) {
					return Double.MAX_VALUE - weightRows * countsS[ip1.getFirst()] / weightedDistance;
				} else if (ip1.getSecond() == ip2.getSecond()) {
					return Double.MAX_VALUE - weightColumns * countsS[ip1.getSecond()] / weightedDistance;
				} else {
					return Double.MAX_VALUE;
				}
			} else {
				calcCandidateS.clear();
				calcCandidateT.clear();
				calcCandidateS.addAll(calcPartition1.getCandidateS());
				calcCandidateS.addAll(calcPartition2.getCandidateS());
				calcCandidateT.addAll(calcPartition1.getCandidateT());
				calcCandidateT.addAll(calcPartition2.getCandidateT());
				
				long cost1 = 0L;
				long cost2 = 0L;
				long costMerged = 0L;
				
				// Input cost of partition 1
				for (Integer idxS : calcPartition1.getCandidateS()) {
					cost1 += weightRows * countsS[idxS];
				}
				for (Integer idxT : calcPartition1.getCandidateT()) {
					cost1 += weightColumns * countsT[idxT];
				}
				
				// Input cost of partition 2
				for (Integer idxS : calcPartition2.getCandidateS()) {
					cost2 += weightRows * countsS[idxS];
				}
				for (Integer idxT : calcPartition2.getCandidateT()) {
					cost2 += weightColumns * countsT[idxT];
				}
				
				calcSumInputCostBefore = cost1 + cost2;
				
				// Input cost of merged partition
				for (Integer idxS : calcCandidateS) {
					costMerged += weightRows * countsS[idxS];
				}
				for (Integer idxT : calcCandidateT) {
					costMerged += weightColumns * countsT[idxT];
				}
				
				calcInputCostAfter = costMerged;
				
				return (double) calcInputCostAfter / calcSumInputCostBefore;
			}
		default:
			return Double.MIN_VALUE;
		}
	}
	
	private void mergePartitions(DistanceMeasure dm, int partitionIDRemove, int partitionIDMergeTo) {
		int i1,i2;
		activePartition[partitionIDRemove] = false;
		HashSet<Integer> pairs;
		Integer pair;
		for (int i = 0 ; i < numCandidateCells; ++i) {
			if (!activePartition[i]){
				continue;
			}
			
			if(i < partitionIDRemove) {
				i1 = i; // partitions[i].getId();
				i2 = partitionIDRemove; // partitions[partitionIDRemove].getId();
			} else {
				i1 = partitionIDRemove; // partitions[partitionIDRemove].getId();
				i2 = i; // partitions[i].getId();
			}
			//pair = new IntPair();
			//pair.set(i1,i2);
			pair = i1*partitionsCountInitial + i2;
			
			
			pairs = distancesTotal.get(distancePairs[i1][i2]);
			pairs.remove(pair);
			if (pairs.size() == 0) {
				distancesTotal.remove(distancePairs[i1][i2]);
			} else {
				distancesTotal.put(distancePairs[i1][i2],pairs);
			}
		}
		
		/*	We will merge the second into the first (the second will be deleted)	*/
		partitions[partitionIDMergeTo].addPartition(partitions[partitionIDRemove]);
		
		partitions[partitionIDRemove] = null;
		
		for (int i = 0 ; i < numCandidateCells ; ++i) {
			if(!activePartition[i] || i == partitionIDMergeTo){
				continue;
			}
			
			if (i < partitionIDMergeTo) {
				i1 = i; // partitions[i].getId();
				i2 = partitionIDMergeTo; // partitions[partitionIDMergeTo].getId();
			} else {
				i1 = partitionIDMergeTo; // partitions[partitionIDMergeTo].getId();
				i2 = i; // partitions[i].getId();
			}
			//pair = new IntPair();
			//pair.set(i1,i2);
			
			pair = i1*partitionsCountInitial + i2;
			
			/*	distancePairs[i1][i2] = Double.MIN_VALUE;	*/
			pairs = distancesTotal.get(distancePairs[i1][i2]);
			pairs.remove(pair);
			if (pairs.size() == 0) {
				distancesTotal.remove(distancePairs[i1][i2]);
			} else {
				distancesTotal.put(distancePairs[i1][i2],pairs);
			}
			
			distancePairs[i1][i2] = calculatePartitionsDistance(dm, i1, i2, false);
			
			pairs = distancesTotal.get(distancePairs[i1][i2]);
			if (pairs == null) {
				pairs = new HashSet<Integer>();
			}
			pairs.add(pair);
			distancesTotal.put(distancePairs[i1][i2], pairs);
		}
	}
	
	/**
	 *  Until less than or equal to numPartitions 
	 */
	private boolean processMerging(DistanceMeasure dm) {
		int partitionID1,partitionID2;
		long tmpInputCost, tmpCandidateCells;
		int partitionIDRemove,partitionIDMergeTo;
		HashSet<Integer> candidateS = new HashSet<Integer>(),candidateT = new HashSet<Integer>();
		while (partitionsCount > numPartitions){
			partitionIDRemove = Integer.MIN_VALUE;
			partitionIDMergeTo = Integer.MIN_VALUE;
			
			 
			boolean merge = false;
			/*	entryDistancePartitionPairs holds all the distances in the form <distance,<partition_id,partition_id>>	*/
			loop2:for (Entry<Double,HashSet<Integer>> entryDistancePartitionPairs : distancesTotal.entrySet()) {
				for (Integer pair : entryDistancePartitionPairs.getValue()) {
					//partitionID1 = pair.getFirst();
					//partitionID2 = pair.getSecond();
					partitionID1 = pair / partitionsCountInitial;
					partitionID2 = pair % partitionsCountInitial;
					
					switch(bsp) {
						case MAX_PARTITION_INPUT:
					
							candidateS.clear();
							candidateS.addAll(partitions[partitionID1].getCandidateS());
							candidateS.addAll(partitions[partitionID2].getCandidateS());
							
							candidateT.clear();
							candidateT.addAll(partitions[partitionID1].getCandidateT());
							candidateT.addAll(partitions[partitionID2].getCandidateT());
							
							tmpInputCost = 0L;
							for (Integer idxS : candidateS) {
								tmpInputCost += countsS[idxS];
							}
							for (Integer idxT : candidateT) {
								tmpInputCost += countsT[idxT];
							}
							
							
							/*	We should not merge these two partitions. The final input cost is too large.	*/
							if (tmpInputCost > maxPartitionInput) {
								continue;
							}
							break;
						case MAX_PARTITION_CANDIDATE_CELLS:
							HashSet<IntPair> cells = new HashSet<IntPair>();
							
							cells.addAll(partitions[partitionID1].getCandidateCells());
							cells.addAll(partitions[partitionID2].getCandidateCells());
							
							tmpCandidateCells = cells.size();
							
							if(tmpCandidateCells > maxPartitionCandidateCells) {
								continue;
							}
							break;
					}
				
					partitionIDRemove = partitionID2;
					partitionIDMergeTo = partitionID1;
					
					merge = true;
					
					break loop2;
				}
			}
			if (!merge) {
				return false;
				
			} else {
				/*	For debugging purposes	*/
//				System.out.println("removeID: " + partitionIDRemove + " mergeToID: " + partitionIDMergeTo);
				mergePartitions(dm,partitionIDRemove,partitionIDMergeTo);
				--partitionsCount;
			}
		}
		return true;
	}

	private long binarySearch(DistanceMeasure dm,  long lowerBound, long upperBound) throws PartitioningError {

		if (lowerBound > upperBound) {
			String errorMessage = "Error when searching for the right max. LowerBound is bigger than UpperBound."
					+ "(" + lowerBound + ">" + upperBound + ")" + " No max input returned a true value.";
			throw new PartitioningError(errorMessage);
		}

		/*	calculate midpoint to cut set in half	*/
		long mid = (lowerBound + upperBound) / 2;
		
		/*	For debugging purposes	*/
//		System.out.println("mid: " + mid + " lowerBound: " + lowerBound + " upperBound: " + upperBound);

		cloneInitialPartitionsDistances();
		
		switch(bsp) {
			case MAX_PARTITION_INPUT:
				maxPartitionInput = mid;
				break;
			case MAX_PARTITION_CANDIDATE_CELLS:
				maxPartitionCandidateCells = mid;
				break;
		}

		boolean result = processMerging(dm);
		
		/* Freeing memory after some binary searches--- Increase the step accordingly	*/
		if (++memoryCounter == 50) {
			long freeMemory = Runtime.getRuntime().freeMemory();
			if ((double) freeMemory / maxMemory <= 0.2) {
				System.runFinalization();
				System.gc();
			}
			memoryCounter = 0;
		}

		if (!result) {
			return binarySearch(dm, mid + 1, upperBound);
		} else {

			if (lowerBound == mid) {
				return mid;
			}

			return binarySearch(dm, lowerBound, mid);
		}
	}

	/**
	 * Used only for debugging purposes
	 */
	private void printElementDistances() {
		System.out.println("Printing elements!");
		
		int i1,i2;
		int counter1=0,counter2;
		for (int i = 0 ; i < partitionsCount ; ++i) {
			PartitionNonRectangular partitionNR = partitions[i];
			
			System.out.println("Partition ID: " + counter1);
			System.out.println("\t cells in partition: ");
			Iterator<IntPair> itCells = partitionNR.getCandidateCells().iterator();
			while (itCells.hasNext()) {
				IntPair cellIndex = itCells.next();
				
				System.out.println("\trow: " + cellIndex.getFirst() + ", column: " + cellIndex.getSecond());
			}
			System.out.println("");
			
			System.out.println("\t indicesS: ");
			System.out.println("\t " + partitionNR.getCandidateS().toString());
			System.out.println("\t indicesT: ");
			System.out.println("\t " + partitionNR.getCandidateT().toString());
			System.out.println("");
			
			System.out.println("\t distances from other partitions: ");
			
			TreeMap<Double,HashSet<Integer>> distanceFromOthers = new TreeMap<Double,HashSet<Integer>>();
			counter2 = 0;
			for (int j = 0 ; j < partitionsCount ; ++j) {
				if (j == i) {
					continue;
				}
				
				if (i < j) {
					i1 = partitions[i].getId();
					i2 = partitions[j].getId();
				} else {
					i1 = partitions[j].getId();
					i2 = partitions[i].getId();
				}
				
				double distance = distancePairs[i1][i2];
				HashSet<Integer> others = distanceFromOthers.get(distance);
				if( others == null ){
					others = new HashSet<Integer>();
				}
				others.add(counter2);
				distanceFromOthers.put(distance, others);
				counter2++;
			}
			for (Entry<Double,HashSet<Integer>> entry : distanceFromOthers.entrySet()) {
				if(entry.getKey() == Double.MAX_VALUE) {
					continue;
				}
				for (Integer other : entry.getValue()){
					System.out.println("\t distance: " + String.format("%.15f",entry.getKey()) + " partitionID: " + other );
				}
			
			}
		}
		counter1++;
	}
	
	/*
	 * Getters - Setters
	 */

	@Override
	public PartitionNonRectangular[] getPartitions() {
		return partitions;
	}
	@Override
	public Integer getPartitionsCount() {
		return partitionsCount;
	}
	public double[][] getDistancePairs() {
		return distancePairs;
	}
}
