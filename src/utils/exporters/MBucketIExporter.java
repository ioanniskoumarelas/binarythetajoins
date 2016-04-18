/**
 * MBucketIExporter.java
 * 
 * Exports data and statistics about the MBucketI
 * execution phase.
 * 
 * @author John Koumarelas
 */

package utils.exporters;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import datatypes.LongTriple;


public class MBucketIExporter {
	
	public void exportCountersCSV(HashMap<String, LongTriple> countersMBucketIReducersInOut, Path counters) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(counters,true)));
			
			/*	TODO: add getRegionsIDs to utils.handlers.partitionMatrix.PartitionMatrixRegions	*/
			for (Entry<String,LongTriple> entry : countersMBucketIReducersInOut.entrySet()) {
				LongTriple inOut = entry.getValue();
				out.write(entry.getKey() + "," + inOut.getFirst() + "," + inOut.getSecond() + "," + inOut.getThird());
				out.newLine();
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportMBucketIStatisticsCSV(long sizeS, long sizeT, 
			HashMap<String, LongTriple> countersMBucketIPartitionsInOut, Path partitionsStatistics) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());

			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(partitionsStatistics, true)));
			

			long[] inputCosts = new long[countersMBucketIPartitionsInOut.size()];
			
			int i = 0;
			for (Entry<String,LongTriple> entry : countersMBucketIPartitionsInOut.entrySet()) {
				inputCosts[i++] = entry.getValue().getFirst();
			}
			
//			out.write("replicationRate" + "," + new ReplicationRateInputCostMetric(sizeS,sizeT,inputCosts).getReplicationRate());
//			out.newLine();
//			
//			out.write("imbalance" + "," + new ImbalanceInputCostMetric(inputCosts).getImbalance());
//			out.newLine();
			
			long maxPartitionInput = Long.MIN_VALUE;
			for (long inputCost : inputCosts){
				if (inputCost > maxPartitionInput) {
					maxPartitionInput = inputCost;
				}
			}
			
			out.write("maxPartitionInput" + "," + maxPartitionInput);
			out.newLine();
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportExecutionTimes(
			long executionTimeMBucketI,
			Path executionTimes) throws IOException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		/*	executionTimes.csv	*/
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(executionTimes,true)));
		
		out.write("MBucketI" + "," + String.valueOf(executionTimeMBucketI));
		out.close();
	}
	
}
