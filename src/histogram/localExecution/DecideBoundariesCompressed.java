/**
 * DecideBoundariesCompressed.java
 * 
 * This class provides methods for deciding which should 
 * be the boundaries of the histogram, given a list of values.
 * 
 * The decision of the boundaries implements the Equi-Depth 
 * histograms.
 * 
 * @author John Koumarelas
 */

package histogram.localExecution;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;

import datatypes.exceptions.HistogramsError;

public class DecideBoundariesCompressed {

	private int uniqueValues;
	private long[] cellValue;
	private long[] cellFrequency;
	private double meanLoadFactor;
	
	public class Bucket {
		
		private int from;
		private int to;
		private long fSum;

		public Bucket() {
			from = Integer.MAX_VALUE;
			to = Integer.MIN_VALUE;
			fSum = 0;
		}
		
		public Bucket(int from, int to, long fSum) {
			this.from = from;
			this.to = to;
			this.fSum = fSum;
		}
		
		protected Bucket clone() {
			return new Bucket(from, to , fSum);
		}
		
		public void insertCell(Integer index, Long frequency) throws HistogramsError {			
			if (index < from){
				from = index;
				if (fSum == 0){
					to = from;
				}
			} else if (index > to){
				to = index;
				if (fSum == 0){
					from = to;
				}
			} else {
				String errorMessage =	"ERROR - DecideBoundariesCompressed.Bucket#insertCell(), " + 
										"Should not happen. All cells added must be at the start " +
										"or end of the bucket, not between.";
				throw new HistogramsError(errorMessage);
			}
			fSum += frequency;
		}
		
		public Long removeCell(Integer index, Long frequency) throws HistogramsError{
			
			fSum -= frequency;
			if (index == from) {
				if (fSum == 0){
					from = Integer.MAX_VALUE;
					to = Integer.MIN_VALUE;
				} else {
					++from;
				}
			} else if (index == to){
				if (fSum == 0){
					from = Integer.MAX_VALUE;
					to = Integer.MIN_VALUE;
				} else {
					--to;
				}
			} else {
				String errorMessage = 	"ERROR - DecideBoundariesCompressed.Bucket#removeCell()" + 
										"Should not happen. All cells removed must be either at" +
										" the start or at end of the bucket, not between.";
				throw new HistogramsError(errorMessage);
			}
			
			return frequency;
		}
		
		@Override
		public String toString() {
			return from + "," + to + "," + fSum;
		}
		
		public int getFrom() {
			return from;
		}

		public int getTo() {
			return to;
		}

		public long getfSum() {
			return fSum;
		}
	}
	
	public class AssignmentResult {
		private Bucket[] buckets;
		private double rmse;
		
		public Bucket[] getBuckets() {
			return buckets;
		}

		public double getRmse() {
			return rmse;
		}

		public AssignmentResult(Bucket[] buckets,double rmse) {
			this.buckets = buckets;
			this.rmse = rmse;
		}
		
		protected AssignmentResult clone(){
			return new AssignmentResult(buckets, rmse);
		}
		
		@Override
		public String toString() {
			return Arrays.deepToString(buckets) + "," + rmse;
		}
	}
	
	private double calculateRMSE(Bucket[] buckets, double mean){
		double sumSE = 0.0;
		for (Bucket bucket : buckets){
			sumSE += Math.pow(bucket.getfSum() - mean, 2.0);
		} 
		double mse = sumSE; // sumSE / buckets.length;
		
		double rmse = mse; // Math.sqrt(mse);
		
		return rmse;
	}
	
	private AssignmentResult findOptimalAssignment(Bucket[] buckets) throws HistogramsError {
		/*	Move buckets where possible	*/
		
		double rmseBefore = calculateRMSE(buckets, meanLoadFactor);
		
		/*	Method 1.	*/
		/*	Left to right, check whether you can get a cell from a right bucket to its' left bucket.	*/
		for (int i = 0 ; i < buckets.length - 1; ++i) {
			if (buckets[i].getfSum() < buckets[i+1].getfSum()){
				int swapCellIdx = buckets[i+1].getFrom();
				long swapCellFreq = cellFrequency[swapCellIdx];
				
				/*	What-if scenario	*/
				long leftF = buckets[i].getfSum() + swapCellFreq;
				long rightF = buckets[i+1].getfSum() - swapCellFreq;
				/* if((leftF <= rightF) || (leftF <= meanLoadFactor)) { */ // after the transfer, the left bucket will still have less load.
				
				double lrMSEBefore = Math.pow(buckets[i].getfSum() - meanLoadFactor, 2.0) +Math.pow(buckets[i+1].getfSum() - meanLoadFactor, 2.0);
				double lrMSEAfter = Math.pow(leftF - meanLoadFactor,2.0) + Math.pow(rightF - meanLoadFactor,2.0); 
				
				double newRMSE = rmseBefore - lrMSEBefore + lrMSEAfter;
			
				if (newRMSE < rmseBefore) {
					/*	Perform the transfer	*/
					Bucket[] newBuckets = new Bucket[buckets.length];
					for (int b_i = 0 ; b_i < buckets.length ; ++b_i){
						newBuckets[b_i] = buckets[b_i].clone();
					}
					
					newBuckets[i].insertCell(swapCellIdx, swapCellFreq);
					newBuckets[i+1].removeCell(swapCellIdx, swapCellFreq);
					
					double rmseAfter = calculateRMSE(newBuckets, meanLoadFactor);
					
					AssignmentResult arRec = findOptimalAssignment(newBuckets);
					if (arRec.getRmse()< rmseAfter) {
						if ( arRec.getRmse() < rmseBefore) {
							System.out.println("rmseBefore: " + rmseBefore + "\t" + "rmseRecursionAfter: " + arRec.getRmse());
							return arRec;
						}
					} else {
						if (rmseAfter < rmseBefore) {
							System.out.println("rmseBefore: " + rmseBefore + "\t" + "rmseAfter: " + rmseAfter);
							AssignmentResult ar = new AssignmentResult(newBuckets,rmseAfter);
							return ar;
						}
					}
				}
			}
		}
			
		AssignmentResult ar = new AssignmentResult(buckets,rmseBefore);
		return ar;
	}
	
	private AssignmentResult optimalAssignment(int k) throws HistogramsError {
		Bucket[] buckets = new Bucket[k];
		for(int i = 0 ; i < k ; ++i) {
			buckets[i] = new Bucket();
		}
		
		/*	2.2. At first we will assign a cell to all the buckets, except from the last where we will assign all the rest ones.	*/
		//		Then we will start balancing.
		for(int i = 0 ; i < k-1; ++i){
			buckets[i].insertCell(i, cellFrequency[i]);
		}
		// ... For the rest bucket assign all the remainder
		for(int i = k-1; i < uniqueValues; ++i) {
			buckets[k-1].insertCell(i, cellFrequency[i]);
		}
		
		// 2.3. Now the most difficult part of the algorithm. Start balancing/transferring the cells between the buckets.
		// (without breaking the continuous nature of the ranges - each bucket will have a continuous range of values)
		
		// 2.3.1. Using RMSE -- Root Mean Square Error
		long cardinality = 0L;
		for(int i = 0 ; i < uniqueValues ; ++i) {
			cardinality += cellFrequency[i];
		}

		meanLoadFactor = (double) cardinality / k;
		
		return findOptimalAssignment(buckets);
	}
	
	private AssignmentResult stepAssignment(int k) throws HistogramsError {
		
		long cardinality = 0L;
		for(int i = 0 ; i < uniqueValues ; ++i) {
			cardinality += cellFrequency[i];
		}
		meanLoadFactor = (double) cardinality / k;
		
		boolean assignedBuckets = false; // Have all buckets been assigned at least one cell?
		
		Bucket[] buckets = null;
		
		/* 	It is used like a step. We will gradually decrease this amount to fit the	*/ 
		/*	cells to all k buckets, in cases where there is great skewness.	*/
		double curMeanLoadFactor = meanLoadFactor;
		while(!assignedBuckets) {
			
			/*	Initialization of buckets	*/
			buckets = new Bucket[k];
			for(int i = 0 ; i < k ; ++i) {
				buckets[i] = new Bucket();
			}
						
			/*	Trying to fit the cells to the buckets	*/
			for(int i = 0, b_i = 0 ; i < k && b_i < uniqueValues;) {
				if( buckets[i].getfSum() < curMeanLoadFactor) { // There is space for another bucket
					buckets[i].insertCell(b_i, cellFrequency[b_i]);
					++b_i;
				} else {
					++i;
				}
			}
			
			if(buckets[k-1].getfSum() > 0){
				assignedBuckets = true;
			} else {
				--curMeanLoadFactor;
			}
		}
		
		double rmse = calculateRMSE(buckets, meanLoadFactor);
		AssignmentResult ar = new AssignmentResult(buckets,rmse);
		
		return ar;
	}
	
	
	/**
	 * Please note that currently we do not want to split a cell to two buckets, therefore we have a slight variation of Equi-Depth Histograms,
	 * where not all buckets have exactly the same amount of tuples, but close to the same. This method performs the necessary mapping of cells to buckets.
	 */
	public AssignmentResult decideIndexes(TreeMap<Long,Long> cellsFrequencies, int k) throws IOException, InterruptedException, HistogramsError {
		/*	1. Some validations that enough values are given.	*/
		
		/*	1.1. Are the different values given at least k?	*/
		if(cellsFrequencies.size() < k) {
			String errorMessage = "ERROR - DecideBoundariesCompressed#decideIndexes(), Not enough cells are given";
			throw new HistogramsError(errorMessage);
		}
			
		/*  1.2. Are the frequencies at least k? (isn't this case catched by the previous rule?)
		long sumFreqs = 0;
		Iterator<Entry<Long,Long>> itTM = cellsFrequencies.entrySet().iterator();
		while(itTM.hasNext()) {
			Entry<Long,Long> entry = itTM.next();
			
			sumFreqs += entry.getValue();
		}
		if(sumFreqs < k) {
			System.out.println("\n*** ERROR *** [histogram1.reducers.decideIndexesNew] Cell frequencies of tuples arrived less than k. ***");
			System.exit(-1);
		} */		
		
		/*	2. The actual Buckets	*/
		
		/*	2.1. Copy the values to primitive arrays, and now we will use indexes to these arrays to know when a cell belongs to a bucket. (If his indexes include that cell)	*/
		uniqueValues = cellsFrequencies.size();
		cellValue = new long[uniqueValues];
		cellFrequency = new long[uniqueValues];
		int i = 0 ;
		for(Entry<Long,Long> cell : cellsFrequencies.entrySet()){
			cellValue[i] = cell.getKey();
			cellFrequency[i] = cell.getValue();
			++i;
		}
		
		/*	TODO: Achieve a better assignment of the cells to the histograms.	*/
		//	AssignmentResult ar = optimalAssignment(k); 
		
		/*	A quick heuristic to assign the cells to the histograms.	*/
		AssignmentResult ar = stepAssignment(k); 
		
		return ar;
	}
	
	public static void main(String[] args) throws HistogramsError {
		TreeMap<Long,Long> cellsFrequencies = new TreeMap<Long,Long>();
		int k = 1000;
		
		for (long i = 0 ; i < 10000; ++i) {
			cellsFrequencies.put(i,i);
		}
		
		try {
			DecideBoundariesCompressed dbc = new DecideBoundariesCompressed();
			
			AssignmentResult ar = dbc.decideIndexes(cellsFrequencies, k);
			
			Bucket[] buckets = ar.getBuckets();
			
			System.out.println("Mean Load Factor: " + dbc.meanLoadFactor);
			for (int i = 0 ; i < buckets.length; ++i) {
				System.out.println(buckets[i].getFrom() + "," + buckets[i].getTo() + "," + buckets[i].getfSum() + "\t" + ar.getRmse());
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public long[] getCellValue() {
		return cellValue;
	}
	
}
