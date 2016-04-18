/**
 * Partitioner.java
 * 
 * It is an abstract class that represents the partitionings
 * that will be implemented with different methods.
 * 
 * @author John Koumarelas
 */

package partitioning;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import model.PartitionMatrix;
import model.partitioning.Partition;
import partitioning.clustering.MergingPartitionToPartition;
import datatypes.DoubleQuad;
import datatypes.exceptions.PartitioningError;
import datatypes.partitioning.MetricsScore;

public abstract class Partitioner {

	public enum SearchPolicy {
	    RANGE_SEARCH("RANGE_SEARCH"),
	    BINARY_SEARCH("BINARY_SEARCH");
	    
	    String policy;
	    
	    private SearchPolicy (String policy) {
			this.policy = policy;
		}
	    
	    @Override
	    public String toString() {
	    	return policy;
	    }
	}
	
	/*
	 * Input variables
	 */
	
	public enum BinarySearchPolicy {
	    MAX_PARTITION_INPUT("MAX_PARTITION_INPUT"),
	    MAX_PARTITION_CANDIDATE_CELLS("MAX_PARTITION_CANDIDATE_CELLS"),;
	    
	    String policy;
	    
	    private BinarySearchPolicy(String policy) {
			this.policy = policy;
		}
	    
	    @Override
	    public String toString() {
	    	return policy;
	    }
	}

	protected PartitionMatrix pm = null;
	
	protected long[][] matrix = null;

	protected long[] countsS = null;
	protected long[] countsT = null;

	protected long sizeS = Long.MIN_VALUE;
	protected long sizeT = Long.MIN_VALUE;
	
	protected String partitioningPolicy;
	
	protected int numPartitions = Integer.MIN_VALUE;
	
	protected BinarySearchPolicy bsp;
	
	/*
	 * Temp variables
	 */
	
	protected int numCandidateCells;
	
	/*
	 * Output variables
	 */
	
	protected long executionTimeBinarySearch= Long.MIN_VALUE;
	protected long executionTimeRangeSearch = Long.MIN_VALUE;

	protected long maxPartitionInput = Long.MIN_VALUE;
	protected long maxPartitionCandidateCells = Long.MIN_VALUE;
	
	protected Vector<MetricsScore> rangeSearchMetrics= null;
	
	protected HashMap<Integer,HashSet<Integer>> idxToPartitionsS = null;
	protected HashMap<Integer,HashSet<Integer>> idxToPartitionsT = null;
	
	/*
	 * Methods
	 */
	
	protected void calculateIdxToPartitions(Partition[] partitions) {
		for(Partition partition : partitions) {
			if(partition == null) {
				continue;
			}
			for(Integer idxS : partition.getCandidateS()) {
				HashSet<Integer> partitionsMapping = idxToPartitionsS.get(idxS);
				if(partitionsMapping == null){
					partitionsMapping = new HashSet<Integer>();
				}
				partitionsMapping.add(partition.getId());
				idxToPartitionsS.put(idxS,partitionsMapping);
			}
			
			for(Integer idxT : partition.getCandidateT()) {
				HashSet<Integer> partitionsMapping = idxToPartitionsT.get(idxT);
				if(partitionsMapping == null){
					partitionsMapping = new HashSet<Integer>();
				}
				partitionsMapping.add(partition.getId());
				idxToPartitionsT.put(idxT,partitionsMapping);
			}
		}
	}
	
	protected int calculateCandidateCells(long[][] matrix) {
		int nRows = matrix.length;
		int nColumns = matrix[0].length;
		
		int counter = 0;
		for(int row = 0; row < nRows; ++row) {
			for(int column = 0 ; column < nColumns; ++column) {
				if(matrix[row][column] > 0){
					++counter;
				}
			}
		}
		return counter;
	}

	public Partitioner(PartitionMatrix pm, String partitioningPolicy,
		long sizeS, long sizeT, int numPartitions, BinarySearchPolicy bsp) {

		this.pm = pm;

		this.partitioningPolicy = partitioningPolicy;

		this.matrix = pm.getMatrix();

		this.countsS = pm.getCountsS();
		this.countsT = pm.getCountsT();

		this.sizeS = sizeS;
		this.sizeT = sizeT;

		this.numPartitions = numPartitions;
		
		this.bsp = bsp;
	}
	
	public long getExecutionTimeBinarySearch() {
		return executionTimeBinarySearch;
	}
	
	public long getExecutionTimeRangeSearch() {
		return executionTimeRangeSearch;
	}
	
	public long getMaxPartitionInput() {
		return maxPartitionInput;
	}
	
	public long getMaxPartitionCandidateCells() {
		return maxPartitionCandidateCells;
	}
	
	public HashMap<Integer, HashSet<Integer>> getIdxToPartitionsS() {
		return idxToPartitionsS;
	}

	public HashMap<Integer, HashSet<Integer>> getIdxToPartitionsT() {
		return idxToPartitionsT;
	}

	public Vector<MetricsScore> getRangeSearchMetrics() {
		return rangeSearchMetrics;
	}
	
	public void setRangeSearchMetrics(Vector<MetricsScore> rangeSearchMetrics) {
		this.rangeSearchMetrics = rangeSearchMetrics;
	}

	public abstract void findLowest() throws PartitioningError;
	public abstract void rangeSearch(long lowerMPI, long upperMPI, int granularity) throws PartitioningError;
	public abstract void execute(long max) throws PartitioningError;
	
	public abstract Partition[] getPartitions();
	public abstract Integer getPartitionsCount();
}
