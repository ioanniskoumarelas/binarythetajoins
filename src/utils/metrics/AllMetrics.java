package utils.metrics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

import datatypes.partitioning.MetricsScore;
import model.partitioning.Partition;

public class AllMetrics {
	
	private Partition[] partitions = null;
	private int partitionsCount = Integer.MIN_VALUE;
	private long sizeS = Long.MIN_VALUE;
	private long sizeT = Long.MIN_VALUE;
	private int bucketsS = Integer.MIN_VALUE;
	private int bucketsT = Integer.MIN_VALUE;
	private long[] countsS = null; 
	
	public AllMetrics(Partition[] partitions, int partitionsCount, long sizeS, long sizeT, int bucketsS, int bucketsT, long[] countsS) {
		this.partitions = partitions;
		this.partitionsCount = partitionsCount;
		this.sizeS = sizeS;
		this.sizeT = sizeT;
		this.bucketsS = bucketsS;
		this.bucketsT = bucketsT;
		this.countsS = countsS;
	}
	
	public static HashMap<String, Double> getMinMetrics(Vector<MetricsScore> rangeSearchMetrics) {
		HashMap<String, Double> minMetrics = new HashMap<String, Double>();
		
		for(Entry<String, Double> entry : rangeSearchMetrics.get(0).getMetrics().entrySet()) {
			minMetrics.put(entry.getKey(), Double.MAX_VALUE);
		}
		
		for (MetricsScore ms : rangeSearchMetrics) {
			for (Entry<String, Double> entry : ms.getMetrics().entrySet()) {
				if(minMetrics.get(entry.getKey()) > entry.getValue()) {
					minMetrics.put(entry.getKey(), entry.getValue());
				}
			}
		}
		
		return minMetrics;
	}
	
	public static HashMap<String, Double> getMaxMetrics(Vector<MetricsScore> rangeSearchMetrics) {
		HashMap<String, Double> maxMetrics = new HashMap<String, Double>();
		
		for(Entry<String, Double> entry : rangeSearchMetrics.get(0).getMetrics().entrySet()) {
			maxMetrics.put(entry.getKey(), Double.MIN_VALUE);
		}
		
		for (MetricsScore ms : rangeSearchMetrics) {
			for (Entry<String, Double> entry : ms.getMetrics().entrySet()) {
				if(maxMetrics.get(entry.getKey()) < entry.getValue()) {
					maxMetrics.put(entry.getKey(), entry.getValue());
				}
			}
		}
		
		return maxMetrics;
	}
	
	public HashMap<String, Double> getAllMetrics() {
		HashMap<String, Double> metrics = new HashMap<String, Double>();
		
		metrics.putAll(getSelfJoinMetrics());
		metrics.putAll(getNormalJoinMetrics());
		
		return metrics;
	}
	
	private HashMap<String, Double> getSelfJoinMetrics() {
		double maxIC = Long.MIN_VALUE;
		double minIC = Long.MIN_VALUE;
		double repIC = Double.MIN_VALUE;
		double meanIC = Double.MIN_VALUE;
		double medianIC = Double.MIN_VALUE;
		double stdevIC = Double.MIN_VALUE;
		double imbIC = Double.MIN_VALUE;
		
		double maxCC = Long.MIN_VALUE;
		double minCC = Long.MIN_VALUE;
		//double repCC = Double.MIN_VALUE; // NO POINT, will always be 1. 
		double meanCC = Double.MIN_VALUE;
		double medianCC = Double.MIN_VALUE;
		double stdevCC = Double.MIN_VALUE;
		double imbCC = Double.MIN_VALUE;
		
		double maxB = Long.MIN_VALUE;
		double minB = Long.MIN_VALUE;
		double repB = Double.MIN_VALUE;
		double meanB = Double.MIN_VALUE;
		double medianB = Double.MIN_VALUE;
		double stdevB = Double.MIN_VALUE;
		double imbB = Double.MIN_VALUE;
		
		double maxBdivCC = Double.MIN_VALUE;
		double minBdivCC = Double.MIN_VALUE;
		// double repBdivCC = Double.MIN_VALUE; // No point. BdivCC is like rep by itself
		double meanBdivCC = Double.MIN_VALUE;
		double medianBdivCC = Double.MIN_VALUE;
		double stdevBdivCC = Double.MIN_VALUE;
		double imbBdivCC = Double.MIN_VALUE;
		
		/////////////////////
		// Get primitive arrays (also needed for median)
		long[] partitionsIC = new long[partitionsCount];
		long[] partitionsCC = new long[partitionsCount];
		long[] partitionsB = new long[partitionsCount];
		double[] partitionsBdivCC = new double[partitionsCount];
		
		int i = 0;
		for (Partition partition: partitions) {
			if (partition == null) {
				continue;
			}
			
			// Generate SelfJoin input cost
			HashSet<Integer> bucketsSJ = new HashSet<Integer>();
			bucketsSJ.addAll(partition.getCandidateS());
			bucketsSJ.addAll(partition.getCandidateT());
			partitionsIC[i] = 0;
			for(Integer bucket : bucketsSJ) {
				partitionsIC[i] += countsS[bucket];
			}
			
			partitionsCC[i] = partition.getCandidateCells().size();
			partitionsB[i] = bucketsSJ.size();
			partitionsBdivCC[i] = (double) partitionsB[i] / partitionsCC[i];
			
			++i;
		}
		Arrays.sort(partitionsIC);
		Arrays.sort(partitionsCC);
		Arrays.sort(partitionsB);
		Arrays.sort(partitionsBdivCC);
		/////////////////////
		minIC = partitionsIC[0];
		minCC = partitionsCC[0];
		minB = partitionsB[0];
		minBdivCC = partitionsBdivCC[0];
		
		maxIC = partitionsIC[partitionsCount - 1];
		maxCC = partitionsCC[partitionsCount - 1];
		maxB = partitionsB[partitionsCount - 1];
		maxBdivCC = partitionsBdivCC[partitionsCount - 1];
		
		double sumIC = 0.0;
		double sumCC = 0.0;
		double sumB = 0.0;
		double sumBdivCC = 0.0;
		for (i = 0 ; i < partitionsCount; ++i) {
			long ic = partitionsIC[i];
			long cc = partitionsCC[i];
			long db = partitionsB[i];
			double dbdivcc = partitionsBdivCC[i];
			
			sumIC += ic;
			sumCC += cc;
			sumB += db;
			sumBdivCC += dbdivcc;
		}
		meanIC = sumIC / partitionsCount;
		meanCC = sumCC / partitionsCount;
		meanB = sumB / partitionsCount;
		meanBdivCC = sumBdivCC / partitionsCount;
		
		imbIC = maxIC / meanIC;
		imbCC = maxCC / meanCC;
		imbB = maxB / meanB;
		imbBdivCC = maxBdivCC / meanBdivCC;
		
		repIC = sumIC / (sizeS);
		repB = sumB / (bucketsS);
		
		/* Second pass for STDEV */
		double sumIC_stdev = 0.0;
		double sumCC_stdev = 0.0;
		double sumB_stdev = 0.0;
		double sumBdivCC_stdev = 0.0;
		for (i = 0 ; i < partitionsCount; ++i) {
			sumIC_stdev += Math.pow(meanIC - partitionsIC[i], 2.0);
			sumCC_stdev += Math.pow(meanCC - partitionsCC[i], 2.0);
			sumB_stdev += Math.pow(meanB - partitionsB[i], 2.0);
			sumBdivCC_stdev += Math.pow(meanBdivCC - partitionsBdivCC[i], 2.0);
		}
		
		stdevIC = sumIC_stdev / partitionsCount;
		stdevCC = sumCC_stdev / partitionsCount;
		stdevB = sumB_stdev / partitionsCount;
		stdevBdivCC = sumBdivCC_stdev / partitionsCount;
		
		if(partitionsCount % 2 == 0) {
			medianIC = (partitionsIC[partitionsCount / 2] + partitionsIC[(partitionsCount / 2) + 1]) / 2.0;
			medianCC = (partitionsCC[partitionsCount / 2] + partitionsCC[(partitionsCount / 2) + 1]) / 2.0;
			medianB = (partitionsB[partitionsCount / 2] + partitionsB[(partitionsCount / 2) + 1]) / 2.0;
			medianBdivCC = (partitionsBdivCC[partitionsCount / 2] + partitionsBdivCC[(partitionsCount / 2) + 1]) / 2.0;
		} else {
			medianIC = partitionsIC[partitionsCount / 2];
			medianCC = partitionsCC[partitionsCount / 2];
			medianB = partitionsB[partitionsCount / 2];
			medianBdivCC = partitionsBdivCC[partitionsCount / 2];
		}
		
		HashMap<String, Double> metrics = new HashMap<String, Double>();
		
		/* Input Cost metrics */
		metrics.put("SJmaxIC", maxIC);
		metrics.put("SJminIC", minIC);
		metrics.put("SJmeanIC", meanIC);
		metrics.put("SJmedianIC", medianIC);
		metrics.put("SJstdevIC", stdevIC);
		metrics.put("SJrepIC", repIC);
		metrics.put("SJimbIC", imbIC);
		
		/* Candidate Cells metrics */
		metrics.put("SJmaxCC", maxCC);
		metrics.put("SJminCC", minCC);
		metrics.put("SJmeanCC", meanCC);
		metrics.put("SJmedianCC", medianCC);
		metrics.put("SJstdevCC", stdevCC);
		//metrics.put("repCC", repCC);
		metrics.put("SJimbCC", imbCC);
		
		/* Buckets metrics */
		metrics.put("SJmaxB", maxB);
		metrics.put("SJminB", minB);
		metrics.put("SJmeanB", meanB);
		metrics.put("SJmedianB", medianB);
		metrics.put("SJstdevB", stdevB);
		metrics.put("SJrepB", repB);
		metrics.put("SJimbB", imbB);
		
		/* Buckets div CC metrics */
		metrics.put("SJmaxBdivCC", maxBdivCC);
		metrics.put("SJminBdivCC", minBdivCC);
		metrics.put("SJmeanBdivCC", meanBdivCC);
		metrics.put("SJmedianBdivCC", medianBdivCC);
		metrics.put("SJstdevBdivCC", stdevBdivCC);
		// metrics.put("repBdivCC", repBdivCC);
		metrics.put("SJimbBdivCC", imbBdivCC);
		
		return metrics;
	}
	
	private HashMap<String, Double> getNormalJoinMetrics() {
		double maxIC = Long.MIN_VALUE;
		double minIC = Long.MIN_VALUE;
		double repIC = Double.MIN_VALUE;
		double meanIC = Double.MIN_VALUE;
		double medianIC = Double.MIN_VALUE;
		double stdevIC = Double.MIN_VALUE;
		double imbIC = Double.MIN_VALUE;
		
		double maxCC = Long.MIN_VALUE;
		double minCC = Long.MIN_VALUE;
		//double repCC = Double.MIN_VALUE; // NO POINT, will always be 1. 
		double meanCC = Double.MIN_VALUE;
		double medianCC = Double.MIN_VALUE;
		double stdevCC = Double.MIN_VALUE;
		double imbCC = Double.MIN_VALUE;
		
		double maxB = Long.MIN_VALUE;
		double minB = Long.MIN_VALUE;
		double repB = Double.MIN_VALUE;
		double meanB = Double.MIN_VALUE;
		double medianB = Double.MIN_VALUE;
		double stdevB = Double.MIN_VALUE;
		double imbB = Double.MIN_VALUE;
		
		double maxBdivCC = Double.MIN_VALUE;
		double minBdivCC = Double.MIN_VALUE;
		// double repBdivCC = Double.MIN_VALUE; // No point. BdivCC is like rep by itself
		double meanBdivCC = Double.MIN_VALUE;
		double medianBdivCC = Double.MIN_VALUE;
		double stdevBdivCC = Double.MIN_VALUE;
		double imbBdivCC = Double.MIN_VALUE;
		
		/////////////////////
		// Get primitive arrays (also needed for median)
		long[] partitionsIC = new long[partitionsCount];
		long[] partitionsCC = new long[partitionsCount];
		long[] partitionsB = new long[partitionsCount];
		double[] partitionsBdivCC = new double[partitionsCount];
		
		int i = 0;
		for (Partition partition: partitions) {
			if (partition == null) {
				continue;
			}
			
			partitionsIC[i] = partition.computeInputCost();
			partitionsCC[i] = partition.getCandidateCells().size();
			partitionsB[i] = partition.getCandidateS().size() + partition.getCandidateT().size();
			partitionsBdivCC[i] = (double) partitionsB[i] / partitionsCC[i];
			
			++i;
		}
		Arrays.sort(partitionsIC);
		Arrays.sort(partitionsCC);
		Arrays.sort(partitionsB);
		Arrays.sort(partitionsBdivCC);
		/////////////////////
		minIC = partitionsIC[0];
		minCC = partitionsCC[0];
		minB = partitionsB[0];
		minBdivCC = partitionsBdivCC[0];
		
		maxIC = partitionsIC[partitionsCount - 1];
		maxCC = partitionsCC[partitionsCount - 1];
		maxB = partitionsB[partitionsCount - 1];
		maxBdivCC = partitionsBdivCC[partitionsCount - 1];
		
		double sumIC = 0.0;
		double sumCC = 0.0;
		double sumB = 0.0;
		double sumBdivCC = 0.0;
		for (i = 0 ; i < partitionsCount; ++i) {
			long ic = partitionsIC[i];
			long cc = partitionsCC[i];
			long db = partitionsB[i];
			double dbdivcc = partitionsBdivCC[i];
			
			sumIC += ic;
			sumCC += cc;
			sumB += db;
			sumBdivCC += dbdivcc;
		}
		meanIC = sumIC / partitionsCount;
		meanCC = sumCC / partitionsCount;
		meanB = sumB / partitionsCount;
		meanBdivCC = sumBdivCC / partitionsCount;
		
		imbIC = maxIC / meanIC;
		imbCC = maxCC / meanCC;
		imbB = maxB / meanB;
		imbBdivCC = maxBdivCC / meanBdivCC;
		
		repIC = sumIC / (sizeS + sizeT);
		repB = sumB / (bucketsS + bucketsT);
		
		/* Second pass for STDEV */
		double sumIC_stdev = 0.0;
		double sumCC_stdev = 0.0;
		double sumB_stdev = 0.0;
		double sumBdivCC_stdev = 0.0;
		for (i = 0 ; i < partitionsCount; ++i) {
			sumIC_stdev += Math.pow(meanIC - partitionsIC[i], 2.0);
			sumCC_stdev += Math.pow(meanCC - partitionsCC[i], 2.0);
			sumB_stdev += Math.pow(meanB - partitionsB[i], 2.0);
			sumBdivCC_stdev += Math.pow(meanBdivCC - partitionsBdivCC[i], 2.0);
		}
		
		stdevIC = sumIC_stdev / partitionsCount;
		stdevCC = sumCC_stdev / partitionsCount;
		stdevB = sumB_stdev / partitionsCount;
		stdevBdivCC = sumBdivCC_stdev / partitionsCount;
		
		if(partitionsCount % 2 == 0) {
			medianIC = (partitionsIC[partitionsCount / 2] + partitionsIC[(partitionsCount / 2) + 1]) / 2.0;
			medianCC = (partitionsCC[partitionsCount / 2] + partitionsCC[(partitionsCount / 2) + 1]) / 2.0;
			medianB = (partitionsB[partitionsCount / 2] + partitionsB[(partitionsCount / 2) + 1]) / 2.0;
			medianBdivCC = (partitionsBdivCC[partitionsCount / 2] + partitionsBdivCC[(partitionsCount / 2) + 1]) / 2.0;
		} else {
			medianIC = partitionsIC[partitionsCount / 2];
			medianCC = partitionsCC[partitionsCount / 2];
			medianB = partitionsB[partitionsCount / 2];
			medianBdivCC = partitionsBdivCC[partitionsCount / 2];
		}
		
		HashMap<String, Double> metrics = new HashMap<String, Double>();
		
		/* Input Cost metrics */
		metrics.put("BJmaxIC", maxIC);
		metrics.put("BJminIC", minIC);
		metrics.put("BJmeanIC", meanIC);
		metrics.put("BJmedianIC", medianIC);
		metrics.put("BJstdevIC", stdevIC);
		metrics.put("BJrepIC", repIC);
		metrics.put("BJimbIC", imbIC);
		
		/* Candidate Cells metrics */
		metrics.put("BJmaxCC", maxCC);
		metrics.put("BJminCC", minCC);
		metrics.put("BJmeanCC", meanCC);
		metrics.put("BJmedianCC", medianCC);
		metrics.put("BJstdevCC", stdevCC);
		//metrics.put("repCC", repCC);
		metrics.put("BJimbCC", imbCC);
		
		/* Buckets metrics */
		metrics.put("BJmaxB", maxB);
		metrics.put("BJminB", minB);
		metrics.put("BJmeanB", meanB);
		metrics.put("BJmedianB", medianB);
		metrics.put("BJstdevB", stdevB);
		metrics.put("BJrepB", repB);
		metrics.put("BJimbB", imbB);
		
		/* Buckets div CC metrics */
		metrics.put("BJmaxBdivCC", maxBdivCC);
		metrics.put("BJminBdivCC", minBdivCC);
		metrics.put("BJmeanBdivCC", meanBdivCC);
		metrics.put("BJmedianBdivCC", medianBdivCC);
		metrics.put("BJstdevBdivCC", stdevBdivCC);
		// metrics.put("repBdivCC", repBdivCC);
		metrics.put("BJimbBdivCC", imbBdivCC);
		
		return metrics;
	}
	
}
