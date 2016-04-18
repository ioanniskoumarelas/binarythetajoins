/**
 * This class is responsible for producing the histograms.
 * It does that by utilizing two MapReduce phases.
 * 
 * - Phase 1: The buckets' boundaries are selected. --> boundaries.csv
 * - Phase 2: The buckets' counts are being computed. --> counts.csv
 * 
 * @author John Koumarelas
 */

package histogram.mapreduce;

import org.apache.hadoop.fs.Path;

public class HistogramCreator {
	
	private long executionTimeEquiDepthHistograms1;
	private long executionTimeEquiDepthHistograms2;
	
	public void createHistogram(long sizeS, long sizeT, String samplingRatio, int buckets, Path dataset, Path boundaries, Path counts) {
		EquiDepthHistograms1Compressed edh1Compressed = new EquiDepthHistograms1Compressed(sizeS, sizeT, samplingRatio, buckets, dataset, boundaries);
		edh1Compressed.run();
		executionTimeEquiDepthHistograms1 = edh1Compressed.getExecutionTime();
		
		EquiDepthHistograms2 edh2 = new EquiDepthHistograms2(buckets, dataset, boundaries,counts);
		edh2.run();
		executionTimeEquiDepthHistograms2 = edh2.getExecutionTime();
	}
	
	public long getExecutionTimeEquiDepthHistograms1() {
		return executionTimeEquiDepthHistograms1;
	}

	public long getExecutionTimeEquiDepthHistograms2() {
		return executionTimeEquiDepthHistograms2;
	}
	
}
