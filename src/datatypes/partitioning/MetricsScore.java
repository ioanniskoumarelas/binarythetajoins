package datatypes.partitioning;

import java.util.HashMap;
import java.util.Map.Entry;

public class MetricsScore implements Comparable<MetricsScore> {

	private HashMap<String, Double> metrics;
	private double score;
	
	public MetricsScore(HashMap<String, Double> metrics, double score) {
		this.metrics = metrics;
		this.score = score;
	}
	
	@Override
	public int compareTo(MetricsScore other) {
		for(Entry<String, Double> kv : metrics.entrySet()) {
			String metric = kv.getKey();
			double value = kv.getValue();
			double valueOther = other.metrics.get(metric);
			
			if (value != valueOther) {
				return value < valueOther ? -1 : 1;
			}
		}
		if( score != other.score ) {
			return score < other.score ? -1 : 1;
		}
		return 0;
	}
	
	public HashMap<String, Double> getMetrics() {
		return metrics;
	}
	
	public Double getScore() {
		return score;
	}
	
	public void setScore(double score) {
		this.score = score;
	}
	
}
