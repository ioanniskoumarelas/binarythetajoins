/**
 * PartitioningExporter.java
 * 
 * This class provides methods for exporting the partitions,
 * statistics and other information about them, that 
 * were formed in a previous step.
 * 
 * @author John Koumarelas
 */
package utils.exporters;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Rectangle;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import model.PartitionMatrix;
import model.partitioning.Partition;
import model.partitioning.mbucketi.PartitionRectangular;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYShapeAnnotation;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RectangleEdge;

import datatypes.DoubleQuad;
import datatypes.IntPair;
import datatypes.partitioning.MetricsScore;
import utils.handlers.partitionMatrix.PartitionMatrixChart;
import utils.handlers.partitioner.RangeSearchMetricsChart;
import utils.metrics.AllMetrics;

public class PartitioningExporter {
	
	private PartitionMatrix pm;
	private Path regionSplitterDirectory;
	
	public PartitioningExporter(PartitionMatrix pm, Path regionSplitterDirectory) {
		this.pm = pm;
		this.regionSplitterDirectory = regionSplitterDirectory;
	}
	
	public void exportPartitionRectangularBoundariesCSV(Path partitionRectangularBoundaries, PartitionRectangular[] regionsArray) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(partitionRectangularBoundaries,true)));
			
			for (int i = 0 ; i < regionsArray.length; ++i) {
				if (regionsArray[i] == null) {
					continue;
				}
				out.write(String.valueOf(regionsArray[i].getId()));
				out.write("," + regionsArray[i].getRbToString());
				if ((i + 1) < regionsArray.length) {
					out.newLine();
				}
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPartitionToHistogramIndicesMappingCSV(Path partitionToHistogramIndicesMapping, Partition[] partitions) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(partitionToHistogramIndicesMapping,true)));
			
			for (int i = 0 ; i < partitions.length; ++i) {
				if(partitions[i] == null) {
					continue;
				}
				
				out.write(String.valueOf(partitions[i].getId()));
				out.write(",S_" + partitions[i].getCandidateS().toString()+",T_"+partitions[i].getCandidateT().toString());
				if ((i + 1) < partitions.length) {
					out.newLine();
				}
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPartitionToCellsMappingCSV(Path partitionToCellsMapping, Partition[] partitions) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(partitionToCellsMapping,true)));
			
			for (int i = 0 ; i < partitions.length; ++i) {
				if (partitions[i] == null) {
					continue;
				}
				
				out.write(String.valueOf(partitions[i].getId()));
				for(IntPair ip : partitions[i].getCandidateCells()) {
					out.write("," + ip.getFirst() + "," + ip.getSecond());
				}
				if ((i + 1) < partitions.length) {
					out.newLine();
				}
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPartitionToPartitionDistanceCSV(Path partitionToPartitionDistance, double[][] distancePairs) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(partitionToPartitionDistance,true)));
			
			for (int i = 0 ; i < distancePairs.length ; ++i) { 
				if ((i + 1) >= distancePairs[0].length){
					break;
				}
				out.write(String.valueOf(i) + ":");
				for (int j = i+1 ; j < distancePairs[0].length ; ++j) {
					out.write("[" + String.valueOf(i) + "," + String.valueOf(j) + "," + String.format("%.16f",distancePairs[i][j]) + "]");
				}
				if ((i + 1) < distancePairs.length){
					out.newLine();
				}
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPartitionsInputCostCSV(Path partitionsInputCost, Partition[] partitions) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(partitionsInputCost,true)));
			
			for (int i = 0 ; i < partitions.length; ++i) {
				if (partitions[i] == null) {
					continue;
				}
				
				out.write(String.valueOf(partitions[i].getId()));
				out.write("," + partitions[i].computeInputCost());
				if ((i+1)<partitions.length) {
					out.newLine();
				}
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void exportHistogramIndexToPartitionsMappingCSV(Path regionsMapping, HashMap<Integer,HashSet<Integer>> idxSToPartitions, 
			HashMap<Integer, HashSet<Integer>> idxTToPartitions) {
		try	{
			FileSystem fs = FileSystem.get(new Configuration());

			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(regionsMapping, true)));
	
			Iterator<Entry<Integer, HashSet<Integer>>> it;
			
			it = idxSToPartitions.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Integer, HashSet<Integer>> entry = it.next();
				Integer bndIndex = entry.getKey();
				HashSet<Integer> partitionSet = entry.getValue(); // partition IDs
	
				out.write("S" + "," + bndIndex.toString() + ",");
				Iterator<Integer> itPartition = partitionSet.iterator();
				while (itPartition.hasNext()) {
					Integer partitionID = itPartition.next();
					out.write(partitionID.toString());
					if (itPartition.hasNext()){
						out.write(",");
					}
				}
				out.newLine();
			}
	
			it = idxTToPartitions.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Integer, HashSet<Integer>> entry = it.next();
				Integer bndIndex = entry.getKey();
				HashSet<Integer> partitionSet = entry.getValue(); // partition IDs
	
				out.write("T" + "," + bndIndex.toString() + ",");
				Iterator<Integer> itPartition = partitionSet.iterator();
				while (itPartition.hasNext()) {
					Integer partitionID = itPartition.next();
					out.write(partitionID.toString());
					if (itPartition.hasNext()){
						out.write(",");
					}
				}
				if (it.hasNext()) {
					out.newLine();
				}
			}
	
			out.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPartitionsStatisticsCSV(long sizeS, long sizeT, int bucketsS, int bucketsT, int partitionsCount, HashMap<String,Double> metrics, Path partitionsStatistics) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());

			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
					fs.create(partitionsStatistics, true)));
			
			TreeMap<String, Double> metricsSorted = new TreeMap<String, Double>();
			metricsSorted.putAll(metrics);
			for(Entry<String, Double> entry : metricsSorted.entrySet()) {
				out.write(entry.getKey() + "," + entry.getValue());
				out.newLine();
			}
			
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportPartitionMatrixRectangularPNG(int buckets, int sparsity, int bands, int bandsOffsetSeed, 
			String rearrangements, String regionSplitterPolicy, int numPartitions, int beaRadius, HashMap<String, Double> metrics,
			Path partitionMatrixRegionsPNG, PartitionRectangular[] regionsArray) {
		try {
			JFreeChart chart = new PartitionMatrixChart(pm).getChart();
			
			StringBuffer sb = new StringBuffer();
			
//			sb.append("---Parameters---" + "\n");
//			
//			sb.append("buckets: " + buckets + "\n");
//			sb.append("sparsity: " + sparsity + "\n");
//			sb.append("bands: " + bands + "\n");
//			sb.append("bandsOffsetSeed: " + bandsOffsetSeed + "\n");
//			sb.append("rearrangements: " + rearrangements + "\n");
//			sb.append("regionSplitterPolicy: " + regionSplitterPolicy + "\n");
//			sb.append("numPartitions: " + numPartitions + "\n");
//			
//			/*	XXX: Information dependent on last rearrangement.	*/
//			if (rearrangements.equals("BEARadius")){
//				sb.append("beaRadius: " + beaRadius + "\n");
//			}
//			
//			sb.append("---Metrics---" + "\n");
//			for(Entry<String, Double> entry : metrics.entrySet()) {
//				sb.append(entry.getKey() + ": " + entry.getValue() + "\n");
//			}
//			
//			TextTitle aggregatedInformation = new TextTitle(sb.toString());
//			aggregatedInformation.setPosition(RectangleEdge.BOTTOM);
//			chart.addSubtitle(aggregatedInformation);
			
			/*	Overlayed regions	*/
			for (PartitionRectangular region : regionsArray) {
				if (region == null) {
					continue;
				}
				
				Rectangle r = new Rectangle(region.getRbStartUpper().getSecond(),region.getRbStartUpper().getFirst(),region.getColumns(),region.getRows());
		        XYShapeAnnotation shape = new XYShapeAnnotation(r,new BasicStroke(3.0f),Color.BLACK);
		        chart.getXYPlot().addAnnotation(shape);
		        
		        XYTextAnnotation text = new XYTextAnnotation(String.valueOf(region.getId()), region.getRbStartUpper().getSecond() + region.getColumns()/2.0, region.getRbStartUpper().getFirst() + region.getRows()/2.0);
		        chart.getXYPlot().addAnnotation(text);
			}
			
			File tempFolderPNG  =  new File("tmp/images/"+regionSplitterDirectory.toString());
			tempFolderPNG.mkdirs();
			File tempFilePNG  =  new File(tempFolderPNG.getPath().toString()+"/pm.png");
			ChartUtilities.saveChartAsPNG(tempFilePNG, chart, 1000, 1400);
			
			FileSystem fs = FileSystem.get(new Configuration());
			fs.copyFromLocalFile(false, true, new Path(tempFilePNG.getPath()), partitionMatrixRegionsPNG);
		}
		catch(IOException e) {
			System.out.println("WARNING - PartitioningExporter#exportPartitionMatrixRectangularPNG, IOException at generating Partition Matrix PNG: " + partitionMatrixRegionsPNG);
			e.printStackTrace();
		}
		catch(Exception e){
			System.out.println("WARNING - PartitioningExporter#exportPartitionMatrixRectangularPNG, Exception at generating Partition Matrix PNG: " + partitionMatrixRegionsPNG);
			e.printStackTrace();
		}
	}

	public void exportExecutionTimes(long executionTimeBinarySearch, long executionTimeRangeSearch, Path executionTimes) throws IOException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		/*	executionTimes.csv	*/
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(executionTimes,true)));
		
		out.write("binarySearch" + "," + String.valueOf(executionTimeBinarySearch)); out.newLine();
		out.write("rangeSearch" + "," + String.valueOf(executionTimeRangeSearch));
		out.close();
	}
	
	public void exportRangeSearchMetricsCSV(Vector<MetricsScore> metricsCases, Path rangeSearchMetricsCSV) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		
		/*	executionTimes.csv	*/
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(rangeSearchMetricsCSV,true)));
		
		for(Entry<String, Double> entry : metricsCases.get(0).getMetrics().entrySet()){
			out.write(entry.getKey() + ",");
		}
		out.write("score");
		
		for (MetricsScore ms : metricsCases) {
			out.newLine();
			for(Entry<String, Double> entry : ms.getMetrics().entrySet()) {
				out.write(entry.getValue() + ",");
			}
			out.write(String.valueOf(ms.getScore()));
		}
		
		out.close();
	}
	
	public void exportRangeSearchMetricsNormalizedCSV(Vector<MetricsScore> metricsCases, HashMap<String, Double> maxMetrics, double maxScore, Path rangeSearchMetricsCSV) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		
		/*	executionTimes.csv	*/
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(rangeSearchMetricsCSV,true)));
		
		for(Entry<String, Double> entry : metricsCases.get(0).getMetrics().entrySet()){
			out.write(entry.getKey() + ",");
		}
		out.write("score");
		
		for (MetricsScore ms : metricsCases) {
			out.newLine();
			for(Entry<String, Double> entry : ms.getMetrics().entrySet()) {
				out.write(entry.getValue() / maxMetrics.get(entry.getKey()) + ",");
			}
			out.write(String.valueOf(ms.getScore() / maxScore));
		}
		
		
		out.close();
	}
	
	public void exportRangeSearchMetricsNormalizedPNG(Vector<MetricsScore> metrics, HashMap<String, Double> maxMetrics, double maxScore, Path rangeSearchMetricsNormalizedXYChartPNG) {
		try {
			JFreeChart xylineChart = new RangeSearchMetricsChart(metrics, maxMetrics,maxScore, rangeSearchMetricsNormalizedXYChartPNG).getChart();

			File tempFolderPNG = new File("tmp/images/" + regionSplitterDirectory.toString());
			tempFolderPNG.mkdirs();
			File tempFilePNG = new File(tempFolderPNG.getPath().toString() + "/rangeSearchMetrics.png");
			ChartUtilities.saveChartAsPNG(tempFilePNG, xylineChart, 800, 700);

			FileSystem fs = FileSystem.get(new Configuration());
			fs.copyFromLocalFile(false, true, new Path(tempFilePNG.getPath()), rangeSearchMetricsNormalizedXYChartPNG);
		} catch (IOException e) {
			
		}
		
		
	}
	
	
	
	
}
