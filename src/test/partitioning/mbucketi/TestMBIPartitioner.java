package test.partitioning.mbucketi;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.Path;

import datatypes.exceptions.PartitioningError;
import model.BucketBoundaries;
import model.PartitionMatrix;
import model.partitioning.Partition;
import model.partitioning.clustering.PartitionNonRectangular;
import model.partitioning.mbucketi.PartitionRectangular;
import partitioning.Partitioner;
import partitioning.mbucketi.MBIPartitioner;
import utils.exporters.PartitionMatrixExporter;
import utils.exporters.PartitioningExporter;
import utils.importers.PartitionMatrixImporter;
import utils.metrics.AllMetrics;

public class TestMBIPartitioner {
	
	private static long[][] getLineMatrix(long[][] matrix, String option, int rows, int columns, int cellGroups) {
		int rowsPerGroup = rows / cellGroups;
		int columnsPerGroup = columns / cellGroups;
		
		for(int k = 0; k < cellGroups; ++k) {
			int rstart = -1;
			int rend = -1;
			int cstart = -1;
			int cend = -1;
			
			if( option.equals("diagonal")) {
				rstart = k * rowsPerGroup;
				rend = (k+1) * rowsPerGroup - 1 - 1;
				
				cstart = k * columnsPerGroup;
				cend = (k+1) * columnsPerGroup - 1 -1;
			} else if( option.equals("horizontal")) {
				rstart = 0 * rowsPerGroup;
				rend = (0+1) * rowsPerGroup - 1 - 1;
				
				cstart = k * columnsPerGroup;
				cend = (k+1) * columnsPerGroup - 1 -1;
			} else if( option.equals("vertical")) {
				rstart = k * rowsPerGroup;
				rend = (k+1) * rowsPerGroup - 1 - 1;
				
				cstart = 0 * columnsPerGroup;
				cend = (0+1) * columnsPerGroup - 1 -1;
			}
			
			for(int i = rstart; i <= rend; ++i) {
				for(int j = cstart; j <= cend; ++j) {
					matrix[i][j] = 2L;
				}
			}
		}
		
		return matrix;
	}
	
	private static long[][] getRandomMatrix(long[][] matrix) {
		Random r = new Random(0);
		
		for(int i = 0; i < matrix.length; ++i) {
			for(int j = 0; j < matrix[0].length; ++j) {
				if( r.nextDouble() < 0.01) {
					matrix[i][j] = 2L;
				}
			}
		}
		
		return matrix;
	}
	
	private static long[][] getBandsRandom(long[][] matrix, int bands) {
		Random r = new Random(0);
		
		int rowsColumnsPerBand = (int) ((matrix.length + matrix[0].length) / (double) bands);
		for(int b = 0; b < bands; ++b) {
			int startPoint = (int) ((b * rowsColumnsPerBand) + (rowsColumnsPerBand * r.nextDouble()));
			
			System.out.println(startPoint);
			
			int startRow = matrix.length - startPoint;
			int startColumn = 0;
			
			if( startPoint > matrix.length) {
				startRow = 0;
				startColumn = startPoint - matrix[0].length;
			}
			
			for(int bi = startRow, bj = startColumn; bi < matrix.length && bj <matrix[0].length; ++bi, ++bj) {
				double value = r.nextDouble();
				int sparsity = (int) (value * 10.0);
				
				for(int sp = 0; sp < sparsity; ++sp) {
					if(sp < sparsity/2) {
						int curRow = bi + (sparsity/2 - sp);
						if( curRow < matrix.length) {
							matrix[curRow][bj] = 2L;
						}
					} else {
						int curColumn = bj + (sp - sparsity/2);
						if( curColumn < matrix[0].length) {
							matrix[bi][curColumn] = 2L;
						}
					}
				}
				
				// matrix[bi][bj] = 2L;
				
				double threshold = 0.7;
				value = r.nextDouble(); 
				if( value < threshold) { // 
					if( value < threshold/2.0 ) {
						++bi;
					} else {
						++bj;
					}
				}
			}
		}
		
		return matrix;
	}
	
	
	private static void performExperiment(String option) throws PartitioningError, IOException {
		int rows = 100;
		int columns = 100;
		long[][] matrix = new long[rows][columns];
		for(long[] matrixRow : matrix) {
			Arrays.fill(matrixRow, 0L);
		}
		
		int cellGroups = 50;
		int numReducers = 50;
		int bands = 5;
		
		if(option.equals("diagonal") || option.equals("horizontal") || option.equals("vertical")) {
			matrix = getLineMatrix(matrix, option, rows, columns, cellGroups);
		} else if(option.equals("random")) {
			matrix = getRandomMatrix(matrix);
		} else if(option.equals("bandsRandom")) {
			matrix = getBandsRandom(matrix, bands);
		}
		
		
		PartitionMatrix pm = new PartitionMatrix();
		long[] countsS = new long[rows];
		long[] countsT = new long[columns];
		Arrays.fill(countsS, 0L);
		Arrays.fill(countsT, 0L);
		
		for(int i = 0; i < rows; ++i) {
			for(int j = 0; j < columns; ++j) {
				if(matrix[i][j] != 0) {
					++countsS[i];
					break;
				}
			}
		}
		
		for(int j = 0; j < columns; ++j) {
			for(int i = 0; i < rows; ++i) {
				if(matrix[i][j] != 0) {
					++countsT[j];
					break;
				}
			}
		}
		
		BucketBoundaries[] boundariesS = new BucketBoundaries[numReducers];
		BucketBoundaries[] boundariesT = new BucketBoundaries[numReducers];
		for(int i = 0; i < numReducers; ++i) {
			BucketBoundaries bb = new BucketBoundaries();
			bb.set(i, i+1);
			boundariesS[i] = bb;
			boundariesT[i] = bb;
		}
		pm.setBoundariesS(boundariesS);
		pm.setBoundariesT(boundariesT);
		
		pm.setCountsS(countsS);
		pm.setCountsT(countsT);
		
		int sizeS = 0;
		for(int i = 0; i < rows; ++i) {
			sizeS += countsS[i];
		}
		
		int sizeT = 0;
		for(int j = 0; j < columns; ++j){
			sizeT += countsT[j];
		}

		pm.setSizeS(sizeS);
		pm.setSizeS(sizeT);
		
		pm.setMatrix(matrix);
		
		MBIPartitioner prt = new MBIPartitioner(pm, "MBI", sizeS, sizeT, numReducers);
		
		prt.findLowest();
		
		// prt.getPartitions(),
		// System.out.println(prt.getPartitions());
		for(PartitionRectangular pr : prt.getPartitions()) {
			System.out.println(pr);
		}
		System.out.println(prt.getPartitionsCount());
		
		int numCandidateCells = 0;
		for(int j = 0; j < columns; ++j) {
			for(int i = 0; i < rows; ++i) {
				if(matrix[i][j] != 0) {
					++numCandidateCells;
				}
			}
		}
		
		String baseDir = "/home/koumarelas/projects/binary_theta_joins/datasets/adhoc_pm/" + option + 
				"_buckets_" + rows +
				"_cellGroups_" + cellGroups +
				"_numCandidateCells_" + numCandidateCells +
				"_numReducers_" + numReducers +
				"_numReducersUsed_" + prt.getPartitionsCount() + "/";
		
		exportPartitioning(prt, pm, numReducers, rows, baseDir);
	}
	
	public static void main(String[] args) throws PartitioningError, IOException {
		// List<String> strings = Arrays.asList("diagonal", "horizontal", "vertical", "random");
//		List<String> strings = Arrays.asList("random");
		List<String> strings = Arrays.asList("bandsRandom");
		
		for(String str : strings) {
			performExperiment(str);
		}
	}
	
	private static void exportPartitioning(Partitioner prt, PartitionMatrix pm, int numPartitions, int rows, String baseDir) throws IOException {
		Path partitioningDirectory = new Path(baseDir);

		Partition[] partitions = prt.getPartitions();

		long executionTimeBinarySearch = prt.getExecutionTimeBinarySearch();
		long executionTimeRangeSearch = prt.getExecutionTimeRangeSearch();

		long[] partitionsInputCosts = new long[prt.getPartitionsCount()];
		int partitionsCounter = 0;
		for (Partition partition : partitions) {
			if (partition == null) {
				continue;
			}
			partitionsInputCosts[partitionsCounter++] = partition.computeInputCost();
		}

		Path histogramIndexToPartitionsMapping = new Path(
				partitioningDirectory.toString() + File.separator + "histogramIndexToPartitionsMapping.csv");
		Path partitionsInputCost = new Path(
				partitioningDirectory.toString() + File.separator + "partitionsInputCost.csv");
		Path partitionsStatistics = new Path(
				partitioningDirectory.toString() + File.separator + "partitionsStatistics.csv");
		Path partitionToCellsMapping = new Path(
				partitioningDirectory.toString() + File.separator + "partitionToCellsMapping.csv");
		// Path partitionToPartitionDistance = new
		// Path(partitioningDirectory.toString() + File.separator +
		// "partitionToPartitionDistance.csv");
		// Path partitionToIndicesMapping = new
		// Path(partitioningDirectory.toString() + File.separator +
		// "partitionToIndicesMapping.csv");
		
		PartitionMatrixExporter pme = new PartitionMatrixExporter(pm,partitioningDirectory);
		pme.exportPartitionMatrixCSV(new Path(baseDir + "/" + "pm.csv"));

		PartitioningExporter pe = new PartitioningExporter(pm, partitioningDirectory);

		Path partitionRectangularBoundaries = new Path(
				partitioningDirectory.toString() + File.separator + "partitionRectangularBoundaries.csv");
		Path partitionMatrixRectangularPNG = new Path(
				partitioningDirectory.toString() + File.separator + "partitionMatrixRectangular.png");
		Path executionTimes = new Path(partitioningDirectory.toString() + File.separator + "executionTimes.csv");

		pe.exportPartitionToCellsMappingCSV(partitionToCellsMapping, partitions);
		// rse.exportPartitionToIndicesMappingCSV(partitionToIndicesMapping,
		// partitions);
		pe.exportHistogramIndexToPartitionsMappingCSV(histogramIndexToPartitionsMapping, prt.getIdxToPartitionsS(),
				prt.getIdxToPartitionsT());
		pe.exportPartitionsInputCostCSV(partitionsInputCost, partitions);

		pe.exportPartitionRectangularBoundariesCSV(partitionRectangularBoundaries,
				(PartitionRectangular[]) partitions);
		
		// else if(partitioningPolicy.equals("AICGM") ||
		// partitioningPolicy.equals("AICGS")) {
		// rse.exportPartitionToPartitionDistanceCSV(partitionToPartitionDistance,
		// ((MergingPartitionToPartition)prt).getDistancePairs());
		// }

		pe.exportPartitionMatrixRectangularPNG(rows, -1, -1, -1, null, null, -1, -1, null, 
				new Path(baseDir + "/pm.png"), (PartitionRectangular[]) prt.getPartitions());
		pe.exportExecutionTimes(executionTimeBinarySearch, executionTimeRangeSearch, executionTimes);
	}
	
}
