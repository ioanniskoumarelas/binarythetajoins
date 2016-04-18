/**
 * RearrangementsExporter.java
 * 
 * Allows exporting of information for the 
 * rearrangements that have taken place.
 * 
 * Methods for exporting the information regarding the rearrangements
 * that have taken place before. Usually the objects are exported in 
 * a CSV (Comma Separated Values) form.
 * 
 * @author John Koumarelas
 */

package utils.exporters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import model.PartitionMatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.title.TextTitle;
import org.jfree.ui.RectangleEdge;

import utils.handlers.partitionMatrix.PartitionMatrixChart;

public class RearrangementsExporter {
	
	private Path partitionMatrixRearrangedDirectory;
	
	public RearrangementsExporter(Path partitionMatrixRearrangedDirectory) {
		this.partitionMatrixRearrangedDirectory = partitionMatrixRearrangedDirectory;
	}

	public void exportRearrangementsCSV(int[] rowsRearrangements, int[] columnsRearrangements, Path rearrangements){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(rearrangements,true)));
			
			out.write("rows");
			for(int i = 0 ; i < rowsRearrangements.length; ++i) {
				out.write("," + rowsRearrangements[i]);
			}
			
			if(columnsRearrangements != null) {
				out.newLine();
				out.write("columns");
				for(int i = 0 ; i < columnsRearrangements.length; ++i) {
					out.write("," + columnsRearrangements[i]);
				}
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPartitionMatrixNormalizedCSV(PartitionMatrix pm, File localTSPk) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(new File(localTSPk.getPath() + File.separator +  "pm.csv"), false));
			
			long[][] matrix = pm.getMatrix();
			
			long maxValue = Long.MIN_VALUE;
			
			// Find maxValue
			for(int i = 0; i < matrix.length; ++i) {
				for(int j = 0;  j < matrix[0].length; ++j) {
					if(matrix[i][j] > maxValue) {
						maxValue = matrix[i][j];
					}
				}
			}
			
			// Export normalized values
			for(int i = 0 ; i < pm.getBucketsS(); ++i) {
				for(int j = 0 ; j < pm.getBucketsT(); ++j) {
					double normalizedValue = 999*((double)matrix[i][j] / maxValue);
					out.write(String.valueOf(normalizedValue));
					if((j+1)<pm.getBucketsT()) {
						out.write(',');
					}
				}
				if((i+1)<matrix.length) {
					out.newLine();
				}
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportExecutionTimes(
			long executionTimeRearrangements,
			Path executionTimes) throws IOException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		/*	executionTimes.csv	*/
		BufferedWriter out = new BufferedWriter(
				new OutputStreamWriter(fs.create(executionTimes,true)));
		
		out.write("rearrangements" + "," + String.valueOf(executionTimeRearrangements));
		out.close();
	}
	
	public void exportPartitionMatrixPNG(PartitionMatrix pm, int buckets, int sparsity, int bands,
			int bandsOffsetSeed, String rearrangementPolicy, int numPartitions, int beaRadius, 
			Path dataset, Path partitionMatrixPNG) {
		try
		{
			JFreeChart chart = new PartitionMatrixChart(pm).getChart();
			
			StringBuffer sb = new StringBuffer();
			
			sb.append("---Parameters---" + "\n");
			
			sb.append("dataset: " + dataset.toString() + "\n");
			sb.append("buckets: " + buckets + "\n");
			sb.append("sparsity: " + sparsity + "\n");
			sb.append("bands: " + bands + "\n");
			sb.append("bandsOffsetSeed: " + bandsOffsetSeed + "\n");
			
			sb.append("rearrangementPolicy: " + rearrangementPolicy + "\n");
			
			/*	XXX: Information dependent on rearrangementPolicy.	*/
			if(rearrangementPolicy.equals("BEARadius")) {
				sb.append("beaRadius: " + beaRadius + "\n");
			} else if(rearrangementPolicy.startsWith("TSPk")) {
				sb.append("numPartitions: " + numPartitions + "\n");
			}
			
			TextTitle aggregatedInformation = new TextTitle(sb.toString());
			aggregatedInformation.setPosition(RectangleEdge.BOTTOM);
			chart.addSubtitle(aggregatedInformation);
			
			File tempFolderPNG  =  new File("tmp/images/"+partitionMatrixRearrangedDirectory.toString());
			tempFolderPNG.mkdirs();
			File tempFilePNG  =  new File(tempFolderPNG.getPath().toString()+"/partitionMatrixRearranged.png");
			ChartUtilities.saveChartAsPNG(tempFilePNG, chart, 500, 700);
			
			FileSystem fs = FileSystem.get(new Configuration());
			fs.copyFromLocalFile(false, true, new Path(tempFilePNG.getPath()), partitionMatrixPNG);
		}
		catch(IOException e) {
			System.out.println("Problem at generating Partition Matrix PNG: " + partitionMatrixPNG);
			e.printStackTrace();
		}
		catch(Exception e){
			System.out.println("Problem at generating Partition Matrix PNG: " + partitionMatrixPNG);
			e.printStackTrace();
		}
	}
	
	public void exportPartitionMatrixEPS(Path partitionMatrixPNG) {
		// TODO
		new UnsupportedOperationException("exportPartitionMatrixEPS not supported yet!");
	}
	
}
