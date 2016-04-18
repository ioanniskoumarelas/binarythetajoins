package utils.handlers.partitioner;

import java.awt.BasicStroke;
import java.awt.Color;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.util.ShapeUtilities;

import datatypes.DoubleQuad;
import datatypes.partitioning.MetricsScore;

public class RangeSearchMetricsChart {
	
	private Vector<MetricsScore> metrics;
	private HashMap<String, Double> maxMetrics;
	private double maxScore;
	private Path rangeSearchMetricsNormalizedXYChartPNG;
	
	public RangeSearchMetricsChart(Vector<MetricsScore> metrics, HashMap<String, Double> maxMetrics, double maxScore,Path rangeSearchMetricsNormalizedXYChartPNG) {
		this.metrics = metrics;
		this.maxMetrics = maxMetrics;
		this.maxScore = maxScore;
		this.rangeSearchMetricsNormalizedXYChartPNG = rangeSearchMetricsNormalizedXYChartPNG;
	}
	
	public JFreeChart getChart() {
		String title = rangeSearchMetricsNormalizedXYChartPNG.toString();
		JFreeChart xylineChart = ChartFactory.createXYLineChart(title,
				"Range Search Case", "Metric Value (normalized)",
				createRangeSearchMetricsDataset(metrics, maxMetrics, maxScore),
				PlotOrientation.VERTICAL, true, true, false);

		ChartPanel chartPanel = new ChartPanel(xylineChart);
		chartPanel.setPreferredSize(new java.awt.Dimension(500, 250));
		final XYPlot plot = xylineChart.getXYPlot();
		
		java.awt.Shape cross = ShapeUtilities.createRegularCross(1.0f, 1.0f);
		java.awt.Shape diamond = ShapeUtilities.createDiamond(3.0f);
		
		XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
		renderer.setSeriesShape(0, cross);
		renderer.setSeriesPaint(0, Color.RED);
		
		renderer.setSeriesShape(1, cross);
		renderer.setSeriesPaint(1, Color.BLUE);
		
		renderer.setSeriesShape(2, cross);
		renderer.setSeriesPaint(2, Color.YELLOW);
		
		renderer.setSeriesShape(3, diamond);
		renderer.setSeriesPaint(3, Color.BLACK);
		renderer.setSeriesStroke(0, new BasicStroke(1.0f));
		renderer.setSeriesStroke(1, new BasicStroke(1.0f));
		renderer.setSeriesStroke(2, new BasicStroke(1.0f));
		renderer.setSeriesStroke(3, new BasicStroke(4.0f));
		plot.setRenderer(renderer);
		// setContentPane( chartPanel );
		
		return xylineChart;
	}
	
	private XYDataset createRangeSearchMetricsDataset(Vector<MetricsScore> metricsCases,HashMap<String, Double> maxMetrics, double maxScore) {
		
		TreeMap<String, XYSeries> pointsMetrics = new TreeMap<String, XYSeries>();
		for(Entry<String, Double> entry : metricsCases.get(0).getMetrics().entrySet()) {
			XYSeries pointsMetric = new XYSeries(entry.getKey());
			pointsMetrics.put(entry.getKey(), pointsMetric);
		}

		XYSeries pointsScore = new XYSeries("Score");

		for (int i = 0; i < metricsCases.size(); ++i) {
			MetricsScore ms = metrics.get(i);

			for(Entry<String, Double> entry : ms.getMetrics().entrySet()) {
				pointsMetrics.get(entry.getKey()).add(i+1, entry.getValue() / maxMetrics.get(entry.getKey()));
			}
			pointsScore.add(i+1, ms.getScore() / maxScore);
		}
		XYSeriesCollection dataset = new XYSeriesCollection();
		
		for(Entry<String, XYSeries> entry : pointsMetrics.entrySet()) {
			dataset.addSeries(entry.getValue());
		}
		dataset.addSeries(pointsScore);
		
		return dataset;
	}
}
