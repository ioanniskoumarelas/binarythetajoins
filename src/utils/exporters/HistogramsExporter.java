/**
 * HistogramsExporter.java
 * 
 * Exports information for the histograms.
 * 
 * @author John Koumarelas
 */

package utils.exporters;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HistogramsExporter {

	public void exportExecutionTimes(
			long executionTimeEquiDepthHistograms1,
			long executionTimeEquiDepthHistograms2,
			Path executionTimes) throws IOException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		/*	executionTimes.csv	*/
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(executionTimes,true)));
		
		out.write("equiDepthHistograms1" + "," + String.valueOf(executionTimeEquiDepthHistograms1));
		out.newLine();
		out.write("equiDepthHistograms2" + "," + String.valueOf(executionTimeEquiDepthHistograms2));
		out.close();
	}
	
}
