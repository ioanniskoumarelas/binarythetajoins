package utils.handlers.partitioner;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

import partitioning.Partitioner;
import utils.metrics.AllMetrics;
import datatypes.partitioning.MetricsScore;

public class PartitionerRangeSearchHandler {
	private double maxScore = Double.MIN_VALUE;
	private double minScore = Double.MAX_VALUE;

	public void calculateBestCaseRangeSearch (Partitioner prt, TreeMap<String, Double> weights) {
		Vector<MetricsScore> rangeSearchMetrics = prt.getRangeSearchMetrics();
		
		// 1st step - find max values for normalizing - find min values as a statistic
		HashMap<String, Double> maxMetrics = AllMetrics.getMaxMetrics(rangeSearchMetrics);
		
		// 2nd step - find triple that minimizes the quantity given the weights.		
		for (MetricsScore ms : rangeSearchMetrics) {
			double score = 0.0;
			HashMap<String, Double> metrics = ms.getMetrics();
			for (Entry<String, Double> entry : weights.entrySet()) {
				score += entry.getValue() * ( metrics.get(entry.getKey()) / maxMetrics.get(entry.getKey()));
			}
			
			ms.setScore(score);
			
			if (score > maxScore) {
				maxScore = score;
			}
		}
		
		// Selection sort
		for (int i = 0; i < rangeSearchMetrics.size() - 1; i++)
        {
            int index = i;
            for (int j = i + 1; j < rangeSearchMetrics.size(); j++) {
                if (rangeSearchMetrics.get(j).getScore() < rangeSearchMetrics.get(index).getScore()) {
                    index = j;
                }
            }
      
            // swap
            MetricsScore ms = rangeSearchMetrics.get(index);
            rangeSearchMetrics.set(index, rangeSearchMetrics.get(i));
            rangeSearchMetrics.set(i, ms);
        }
		
		// Min (best) metrics - score
		minScore = rangeSearchMetrics.get(0).getScore();
		prt.setRangeSearchMetrics(rangeSearchMetrics);
	}

	public double getMinScore() {
		return minScore;
	}

	public double getMaxScore() {
		return maxScore;
	}
}
