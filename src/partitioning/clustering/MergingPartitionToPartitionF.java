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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Vector;

import datatypes.exceptions.PartitioningError;
import datatypes.partitioning.MetricsScore;
import model.PartitionMatrix;
import model.partitioning.Partition;
import model.partitioning.clustering.PartitionNonRectangular;
import partitioning.Partitioner;
import partitioning.clustering.MergingPartitionToPartition.DistanceMeasure;
import utils.metrics.AllMetrics;

public class MergingPartitionToPartitionF extends Partitioner {
	private PartitionNonRectangular[] partitions = null;
	private int partitionsCount = Integer.MIN_VALUE;
	
	private long memoryCounter;
	private long maxMemory;
	/*
	 * Initial
	 */
	private PartitionNonRectangular[] partitionsInitial = null;
	private int partitionsCountInitial = Integer.MIN_VALUE;
	
	private boolean[] activePartition = null;
	
	public MergingPartitionToPartitionF(PartitionMatrix pm, String partitioningPolicy, long sizeS, long sizeT,
										int numPartitions) {
		super(pm,partitioningPolicy,sizeS,sizeT,numPartitions, null);
		this.partitionsCount = 0;
		this.memoryCounter = 0L;
		this.maxMemory = Runtime.getRuntime().maxMemory();
	}
	
	private Double targetF(HashMap<String, Double> allMetrics, HashMap<String, Double> maxMetrics, HashMap<String, Double> minMetrics) {
//		double BJrepIC_normalized = (allMetrics.get("BJrepIC") - minMetrics.get("BJrepIC")) / (maxMetrics.get("BJrepIC") - minMetrics.get("BJrepIC"));
//		double BJmaxIC_normalized = (allMetrics.get("BJmaxIC") - minMetrics.get("BJmaxIC")) / (maxMetrics.get("BJmaxIC") - minMetrics.get("BJmaxIC"));
		
		double BJrepIC_normalized = allMetrics.get("BJrepIC") / maxMetrics.get("BJrepIC");
		double BJmaxIC_normalized = allMetrics.get("BJmaxIC") / maxMetrics.get("BJmaxIC");
		double BJmaxCC_normalized = allMetrics.get("BJmaxCC") / maxMetrics.get("BJmaxCC");
		double BJminIC_normalized = allMetrics.get("BJminIC") / maxMetrics.get("BJminIC");
		double BJimbIC_normalized = allMetrics.get("BJimbIC") / maxMetrics.get("BJimbIC");
		
		return BJimbIC_normalized;
	}
	
	private MetricsScore whatIF_mergeIJ(int i, int j) {
		HashSet<Integer> calcCandidateS = new HashSet<Integer>();
		HashSet<Integer> calcCandidateT = new HashSet<Integer>();
		
		calcCandidateS.addAll(partitions[i].getCandidateS());
		calcCandidateS.addAll(partitions[j].getCandidateS());
		
		calcCandidateT.addAll(partitions[i].getCandidateT());
		calcCandidateT.addAll(partitions[j].getCandidateT());
		
		// Merge j ij (smaller index)
		//partitionsTmp[i].addPartition(partitionsTmp[j]);
		//partitionsTmp[j] = null;
		double BJsumIC = 0.0;
		double BJmaxIC = Double.MIN_VALUE;
		double BJminIC = Double.MIN_VALUE;
		long BJmaxCC = Long.MIN_VALUE;
		for( int k = 0; k < numCandidateCells; ++k) {
			if( !activePartition[k]) {
				continue;
			}
			double BJIC = partitions[k].computeInputCost(); 
			long BJCC = partitions[j].getCandidateCells().size();
			BJsumIC += BJIC;
			if( BJIC > BJmaxIC) {
				BJmaxIC = BJIC;
			}
			if( BJIC < BJminIC) {
				BJminIC = BJIC;
			}
			if( BJmaxCC < BJCC) {
				BJmaxCC = BJCC;
			}
		}
		
		double BJsumMergedIC = 0.0;
		
		for(Integer ijS : calcCandidateS) {
			BJsumMergedIC += countsS[ijS];
		}
		
		for(Integer ijT : calcCandidateT) {
			BJsumMergedIC += countsT[ijT];
		}
		
		if( BJsumMergedIC > BJmaxIC) {
			BJmaxIC = BJsumMergedIC;
		}
		if( BJsumMergedIC < BJminIC) {
			BJminIC = BJsumMergedIC;
		}
		
		BJsumIC += BJsumMergedIC;
		
		// AllMetrics am = new AllMetrics(partitionsTmp, partitionsCount - 1, sizeS, sizeT, 100, 100, countsS);
		double BJrepIC =  BJsumIC / (sizeS+sizeT);
		double BJmeanIC = BJsumIC / partitionsCount;
		double BJimbIC = BJmaxIC / BJmeanIC;
		HashMap<String, Double> metrics = new HashMap<String, Double>();
		metrics.put("BJrepIC", BJrepIC);
		metrics.put("BJmaxIC", BJmaxIC);
		metrics.put("BJmaxCC", (double)BJmaxCC);
		metrics.put("BJminIC", BJminIC);
		metrics.put("BJimbIC", BJimbIC);
		
		return new MetricsScore(metrics, Double.MAX_VALUE);
	}
	
	private void initialize() {
		numCandidateCells = calculateCandidateCells(matrix); // TODO: store it and don't calculate it each time.
		
		this.maxPartitionInput = Long.MIN_VALUE;
		this.partitions = new PartitionNonRectangular[numCandidateCells];
		this.partitionsCount = numCandidateCells;
		
		int nRows = matrix.length;
		int nColumns = matrix[0].length;
				
		int counter = 0;
		for (int row = 0; row < nRows; ++row) {
			for (int column = 0 ; column < nColumns; ++column) {
				if (matrix[row][column] > 0){
					PartitionNonRectangular cg = new PartitionNonRectangular(counter,pm);
					
					cg.addCell(row, column);
					
					partitions[counter] = cg;
					
					++counter;
				}
			}
		}
		activePartition = new boolean[numCandidateCells];
		Arrays.fill(activePartition, true);
	}

	// TODO remove max
	@Override
	public void execute(long max) throws PartitioningError {
		partitions = new PartitionNonRectangular[numCandidateCells];
		initialize();
		
		while( partitionsCount > numPartitions) {
			Vector<AbstractMap.SimpleEntry<MetricsScore, AbstractMap.SimpleEntry<Integer,Integer>>> scores = 
					new Vector<AbstractMap.SimpleEntry<MetricsScore,AbstractMap.SimpleEntry<Integer,Integer>>>();
			for (int i = 0; i < numCandidateCells - 1; ++i) {
				if( !activePartition[i]) {
					continue;
				}
				// Vector<MetricsScore, LinkedList<Integer>> scores = new TreeMap<MetricsScore, LinkedList<Integer>>();
				for (int j = i+1; j < numCandidateCells; ++j) {
					if( !activePartition[j]) {
						continue;
					}
					
					MetricsScore ms = whatIF_mergeIJ(i, j);
					
					scores.add( new java.util.AbstractMap.SimpleEntry<MetricsScore, AbstractMap.SimpleEntry<Integer,Integer>>(ms, 
							new AbstractMap.SimpleEntry<Integer, Integer>(i, j)) );
					// System.out.println("Trying " + i + " with " + j);
				}
			}
			Vector<MetricsScore> scoresOnly = new Vector<MetricsScore>();
			for(Entry<MetricsScore, AbstractMap.SimpleEntry<Integer, Integer>> entry : scores) {
				scoresOnly.add(entry.getKey());
			}
			HashMap<String, Double> maxMetrics = AllMetrics.getMaxMetrics(scoresOnly);
			HashMap<String, Double> minMetrics = AllMetrics.getMinMetrics(scoresOnly);
			
			for(int s = 0 ; s < scores.size(); ++s) {
				scores.get(s).getKey().setScore(targetF(scores.get(s).getKey().getMetrics(), maxMetrics, minMetrics));
			}
			
//			for(int s = 0 ; s < scores.size(); ++s) {
//				System.out.println(scores.get(s).getKey().getScore());
//			}
			
			// Collections.sort(scores, new MetricsScore.MetricsScoreComparatorScores());
//			Collections.sort(scores, new Comparator<AbstractMap.SimpleEntry<MetricsScore, AbstractMap.SimpleEntry<Integer, Integer>>>() {
//				@Override
//				public int compare(AbstractMap.SimpleEntry<MetricsScore, AbstractMap.SimpleEntry<Integer, Integer>> entry1, AbstractMap.SimpleEntry<MetricsScore, AbstractMap.SimpleEntry<Integer, Integer>> entry2) {
//					if(entry1.getKey().getScore() == entry2.getKey().getScore()) {
//						return 0;
//					} else {
//						return entry1.getKey().getScore() > entry2.getKey().getScore() ? 1 : -1;
//					}
//				}
//			});
			
//			for(int s = 0 ; s < scores.size(); ++s) {
//				System.out.println(scores.get(s).getKey().getScore());
//			}
			
			double minScore = Double.MAX_VALUE;
			Integer minScoreI = -1;
			Integer minScoreJ = -1;
			for(int s = 0 ; s < scores.size(); ++s) {
				if( scores.get(s).getKey().getScore() < minScore ) {
					minScore = scores.get(s).getKey().getScore();
					minScoreI = scores.get(s).getValue().getKey();
					minScoreJ = scores.get(s).getValue().getValue();
				}
			}
			
			/* The best score will be now in the first place */
//			AbstractMap.SimpleEntry<Integer, Integer> bestScoreIJ = scores.get(0).getValue();
			
			partitions[minScoreI].addPartition(partitions[minScoreJ]);
			partitions[minScoreJ] = null;
			activePartition[minScoreJ] = false; 
			--partitionsCount;
			
			System.out.println("Merging " + minScoreI + " with " + minScoreJ + " with score: " + minScore);
		}
		
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
	}
	
	public void initialize(DistanceMeasure dm) {
		;
	}
	
	private void mergePartitions(DistanceMeasure dm, int partitionIDRemove, int partitionIDMergeTo) {
		;
	}
	
	/**
	 *  Until less than or equal to numPartitions 
	 */
	private boolean processMerging() {

		return true;
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
		return null;
	}

	@Override
	public void findLowest() throws PartitioningError {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rangeSearch(long lowerMPI, long upperMPI, int granularity) throws PartitioningError {
		// TODO Auto-generated method stub
		
	}
}
