/**
 * PartitionMatrixChart.java
 * 
 * Provides a visual representation of the PartitionMatrix
 * which can then be saved as an image for a quick overview.
 * 
 * @author John Koumarelas
 */

package utils.handlers.partitionMatrix;

import java.io.IOException;

import model.PartitionMatrix;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class PartitionMatrixChart {
	
	private long[][] matrix;
	
	public PartitionMatrixChart(PartitionMatrix pm) {
		this.matrix = pm.getMatrix();
	}

	private XYSeriesCollection createDatasetChart() {
	    XYSeriesCollection result = new XYSeriesCollection();
	    XYSeries series = new XYSeries("Candidate cells");
	    
	    for(int i = 0 ; i < matrix.length; ++i){
	    	for(int j = 0 ; j < matrix[0].length ; ++j) {
	    		if(matrix[i][j] > 0) {
	    			series.add(j,i);
	    		}
	    	}
	    }
	    result.addSeries(series);
	    
	    return result;
	}
	
	public JFreeChart getChart() throws IOException {
		XYSeriesCollection data = createDatasetChart();
		JFreeChart chart = ChartFactory.createScatterPlot(
	            "Scatter Plot", // chart title
	            "T", // x axis label
	            "S", // y axis label
	            data, // data
	            PlotOrientation.VERTICAL,
	            true, // include legend
	            true, // tooltips
	            false // urls
	            );
		
		XYPlot xyPlot = chart.getXYPlot();
		
//		xyPlot.getDomainAxis().setRange(0, matrix.length-1);
//		xyPlot.getRangeAxis().setRange(0, matrix[0].length-1);
		
		xyPlot.getDomainAxis().setRange(-1, matrix.length+1);
		xyPlot.getRangeAxis().setRange(-1, matrix[0].length+1);
		
		chart.setTitle("Partition Matrix");
		
		xyPlot.setRangeAxisLocation(AxisLocation.TOP_OR_LEFT);
		
		xyPlot.setDomainAxisLocation(AxisLocation.TOP_OR_LEFT);
		
		xyPlot.getRangeAxis().setInverted(true);
//		xyPlot.getDomainAxis().setInverted(true);
		
//		NumberAxis yAxis = new NumberAxis();
//		yAxis.
//		yAxis.setRange(Integer.MAX_VALUE, Integer.MIN_VALUE);
//		chart.getXYPlot().setDomainAxis(1, yAxis);
		
		return chart;
	}
	
}
