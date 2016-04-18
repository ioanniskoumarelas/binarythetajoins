/**
 * PartitionMatrixExporter.java
 * 
 * Methods for exporting the PartitionMatrix and other
 * properties of it can be found in this class. Usually
 * the objects are exported in a CSV (Comma Separated Values)
 * form.
 * 
 * @author John Koumarelas
 */

package utils.exporters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map.Entry;

import model.BucketBoundaries;
import model.PartitionMatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.title.TextTitle;
import org.jfree.ui.RectangleEdge;

import utils.handlers.partitionMatrix.PartitionMatrixChart;
import utils.importers.PartitionMatrixImporter;

public class PartitionMatrixExporter {
	
	private PartitionMatrix pm;
	private Path partitionMatrixDirectory;
	
	public PartitionMatrixExporter(PartitionMatrix pm, Path partitionMatrixDirectory) {
		this.pm = pm;
		this.partitionMatrixDirectory = partitionMatrixDirectory;
	}
	
	public void exportPartitionMatrixCSV(Path partitionMatrixCSV) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(partitionMatrixCSV,true)));
			
			long[][] matrix = pm.getMatrix();
			
			for(int i = 0 ; i < matrix.length; ++i) {
				for(int j = 0 ; j < matrix[0].length; ++j) {
					out.write(String.valueOf(matrix[i][j]));
					if((j+1)<matrix[0].length) {
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
	
	public void exportBoundariesCSV(Path boundariesCSV) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(boundariesCSV,true)));
			
			BucketBoundaries[] boundariesS = pm.getBoundariesS();
			BucketBoundaries[] boundariesT = pm.getBoundariesT();
			
			for(int i = 0 ; i < boundariesS.length; ++i) {
				out.write("S" + "," + boundariesS[i].getFrom() + "," + boundariesS[i].getTo());
				if((i+1)<boundariesS.length){
					out.newLine();
				}
			}
			
			out.newLine();
			
			for(int i = 0 ; i < boundariesT.length; ++i) {
				out.write("T" + "," + boundariesT[i].getFrom() + "," + boundariesT[i].getTo());
				if((i+1)<boundariesT.length){
					out.newLine();
				}
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportCountsCSV(Path countsCSV) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(countsCSV,true)));
			
			long[] countsS = pm.getCountsS();
			long[] countsT = pm.getCountsT();
							
			for(int i = 0 ; i < countsS.length; ++i) {
				out.write("S" + "," + i + "," + countsS[i]);
				if((i+1)<countsS.length)
					out.newLine();
			}
			
			out.newLine();
					
			for(int i = 0 ; i < countsT.length; ++i) {
				out.write("T" + "," + i + "," + countsT[i]);
				if((i+1)<countsT.length)
					out.newLine();
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPropertiesCSV(long sizeS, long sizeT, int buckets, int sparsity, int bands, 
			int bandsOffsetSeed, String query,Path dataset, Path properties) {
		
		HashMap<String,String> propertiesMap = new HashMap<String,String>();
		
		propertiesMap.put("dataset", dataset.toString());
		propertiesMap.put("sizeS", String.valueOf(sizeS));
		propertiesMap.put("sizeT", String.valueOf(sizeT));
		propertiesMap.put("buckets", String.valueOf(buckets));
		propertiesMap.put("sparsity", String.valueOf(sparsity));
		propertiesMap.put("bands", String.valueOf(bands));
		propertiesMap.put("bandsOffsetSeed", String.valueOf(bandsOffsetSeed));
		propertiesMap.put("query", query);
		
		exportProperties(propertiesMap, properties);
	}
	
	/**
	 * This is very useful for an optical verification of the PartitionMatrix.
	 * Especially useful during experiments with different methods.
	 */
	public void exportPartitionMatrixPNG(int buckets, int sparsity, int bands,
			int bandsOffsetSeed, Path dataset, Path partitionMatrixPNG) {
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
			
			TextTitle aggregatedInformation = new TextTitle(sb.toString());
			aggregatedInformation.setPosition(RectangleEdge.BOTTOM);
			chart.addSubtitle(aggregatedInformation);
			
			File tempFolderPNG = new File("tmp/images/"+partitionMatrixDirectory.toString());
			tempFolderPNG.mkdirs();
			File tempFilePNG = new File(tempFolderPNG.getPath().toString()+"/partitionMatrix.png");
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
	
	public void exportProperty(String property, String value, Path properties) {
		HashMap<String, String> propertiesMap = new PartitionMatrixImporter().importProperties(properties);
		
		propertiesMap.put(property, value);
		
		exportProperties(propertiesMap,properties);
	}
	
	public void exportProperties(HashMap<String,String> propertiesMap, Path properties) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(properties,true)));
			
			for(Entry<String,String> entry : propertiesMap.entrySet()) {
				out.write(entry.getKey() + "," + entry.getValue());
				out.newLine();
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportExecutionTimes(
			long executionTimePartitionMatrixCreation,
			Path executionTimes) throws IOException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		/*	executionTimes.csv	*/
		BufferedWriter out = new BufferedWriter(
				new OutputStreamWriter(fs.create(executionTimes,true)));
		
		out.write("partitionMatrixCreation" + "," + String.valueOf(executionTimePartitionMatrixCreation));
		out.close();
	}
	
}
