package utils.metrics;

public class ReplicationRateInputCostMetric {
	private long sizeS;
	private long sizeT;
	private long[] groupInputCosts;
	
	public ReplicationRateInputCostMetric(long sizeS, long sizeT, long[] groupInputCosts) {
		this.sizeS = sizeS;
		this.sizeT = sizeT;
		this.groupInputCosts = groupInputCosts;
	}

	public double getReplicationRate() {
		double replicationRate = 0;
				
		long sum = 0;
		
		for (long groupInputCost : groupInputCosts) {
			sum += groupInputCost;
		}
		
		replicationRate = (double) sum / (sizeS + sizeT);
		
		return replicationRate;
	}
}
