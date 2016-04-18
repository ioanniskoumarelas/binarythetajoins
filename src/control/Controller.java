/**
 * Controller.java
 * 
 * 	This class implements the main API of the project BinaryThetaJoins and is part of the job:
 * 		-	"Binary Theta-Joins using MapReduce: Efficiency Analysis and Improvements", by
 * 			Ioannis (John) K. Koumarelas, Athanasios Naskos, Anastasios Gounaris
 * 
 *  From here the user has the ability to:
 *  	-	executionMode=dataGeneration	|	Create a dataset.
 *  	-	executionMode=histograms	|	Produce histograms from a dataset. (2 MapReduce phases)
 *  	-	executionMode=realPartitionMatrix	|	Create a Join Matrix with the candidate cells.
 *  	-	executionMode=rearrangement	|	Rearrange the rows/columns of the matrix. (To improve some metric)
 *  	-	executionMode=partitioning	|	Partition the Join Matrix into partitions.
 *  	-	executionMode=MBucketI	|	Execute the join in a MapReduce environment. (1 MapReduce phase)
 *  
 *  @author John Koumarelas, john.koumarel@gmail.com
 *  
 *  Copyright: This project is published under the MIT license, which is provided in the LICENSE.md accompanying file.
 */

package control;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;

import datatypes.exceptions.JoinException;
import datatypes.exceptions.PartitioningError;
import datatypes.exceptions.RearrangementError;
import histogram.mapreduce.HistogramCreator;
import join.mbi.MBucketI;
import model.BucketBoundaries;
import model.PartitionMatrix;
import model.partitioning.Partition;
import model.partitioning.mbucketi.PartitionRectangular;
import partitioning.Partitioner;
import partitioning.Partitioner.BinarySearchPolicy;
import partitioning.Partitioner.SearchPolicy;
import partitioning.clustering.MergingPartitionToPartition;
import partitioning.clustering.MergingPartitionToPartition.DistanceMeasure;
import partitioning.clustering.MergingPartitionToPartitionF;
import partitioning.mbucketi.MBIPartitioner;
import rearranging.BondEnergyAlgorithm;
import rearranging.BondEnergyAlgorithmRadius;
import rearranging.Rearrangements;
import rearranging.TSPAlgorithm;
import rearranging.TSPAlgorithmTransposedOnDefault;
import rearranging.TSPAlgorithmTransposedOnRearranged;
import utils.exporters.DatasetGeneratorExporter;
import utils.exporters.HistogramsExporter;
import utils.exporters.MBucketIExporter;
import utils.exporters.PartitionMatrixExporter;
import utils.exporters.PartitioningExporter;
import utils.exporters.RearrangementsExporter;
import utils.factories.PartitionMatrixFactory;
import utils.factories.VirtualPartitionMatrixFactory;
import utils.handlers.partitioner.PartitionerRangeSearchHandler;
import utils.importers.DatasetImporter;
import utils.importers.HistogramImporter;
import utils.importers.PartitionMatrixImporter;
import utils.metrics.AllMetrics;

public class Controller {
	/**
	 * This class is composed of two parts:
	 * - Part1: API
	 * - Part2: Execution Mode Methods 
	 */
	
	/*
	 * Part 1 - API
	 * 
	 * This is what the user needs to know in order to call this system.
	 */
	
	/**
	 * This method is responsible for the main execution of the project. For example as to what parameters
	 * should be given, please have a look at the accompanying file: exampleParameters.txt
	 * 
	 * @param args
	 * @throws IOException
	 * @throws PartitioningError: If under executionMode=partitioning, it failed
	 * to partition the PartitionMatrix into partitions.
	 */
	public static void main(String[] args) throws IOException, PartitioningError {
		if (args.length == 0) {
			System.err.println("No arguments specified. Please have a look at exampleParameters.txt for an example of each case (execution mode).");
			System.err.println("Aborting!");
			System.exit(-1);
		}
		
		Controller ctrl = new Controller();
		
		HashMap<String,String> argsMap = new HashMap<String,String>();
		for (int i = 0 ; i < args.length; ++i) {
			String[] argTokens = args[i].split("=");
			argsMap.put(argTokens[0],argTokens[1]);
		}
		
		try {
			if (argsMap.get("executionMode").equals("dataGeneration")) {
				/*	Input	*/
				long generatorSeed = Long.valueOf(argsMap.get("generatorSeed"));
				String generatorMode = argsMap.get("generatorMode");
				
				long from = Long.MIN_VALUE; 
				long to = Long.MIN_VALUE; 
				/* Need to be specified under generatorMode==range */
				long valueReplication = Long.MIN_VALUE;
				if (generatorMode.equals("range")) {
					from = Long.valueOf(argsMap.get("from"));
					to = Long.valueOf(argsMap.get("to"));
					valueReplication = Long.valueOf(argsMap.get("valueReplication"));
				}
				
				/* Need to be specified under generatorMode==distribution */
				long sizeS = Long.MIN_VALUE;
				long sizeT = Long.MIN_VALUE;
				String distribution = null;
				if( generatorMode.equals("distribution")) {
					from = Long.valueOf(argsMap.get("from"));
					to = Long.valueOf(argsMap.get("to"));
					sizeS = Long.valueOf(argsMap.get("sizeS"));
					sizeT = Long.valueOf(argsMap.get("sizeT"));
					distribution = argsMap.get("distribution");
				}
				
				/*	Output	*/
				Path datasetDirectory = new Path(argsMap.get("datasetDirectory")); 

				/*	Execution	*/
				ctrl.executeDataGeneration(from, to, valueReplication, sizeS, sizeT, 
					generatorSeed, generatorMode, distribution,datasetDirectory);
			} else if (argsMap.get("executionMode").equals("histograms")) {
				/*	Input	*/
				int buckets = Integer.valueOf(argsMap.get("buckets"));
				String samplingRatio  = argsMap.get("samplingRatio");
				
				Path dataset = new Path(argsMap.get("dataset"));
				Path datasetSizes = new Path(argsMap.get("datasetSizes"));
				
				/*	Output	*/
				Path histogramsDirectory = new Path(argsMap.get("histogramsDirectory"));
							
				/*	Execution	*/
				ctrl.executeHistograms(buckets,samplingRatio,dataset,datasetSizes,histogramsDirectory);
			} else if (argsMap.get("executionMode").equals("realPartitionMatrix")) {
				/*	Input	*/
				int buckets = Integer.valueOf(argsMap.get("buckets"));
				int sparsity = Integer.valueOf(argsMap.get("sparsity"));
				int bands = Integer.valueOf(argsMap.get("bands"));
				int bandsOffsetSeed = Integer.valueOf(argsMap.get("bandsOffsetSeed"));
				
				Path datasetDirectory = new Path(argsMap.get("datasetDirectory"));
				Path histogramsDirectory = new Path(argsMap.get("histogramsDirectory"));
				
				/*	Output	*/
				Path partitionMatrixDirectory = new Path(argsMap.get("partitionMatrixDirectory"));

				/*	Execution	*/
				ctrl.executePartitionMatrix(buckets, sparsity, bands, bandsOffsetSeed,
					datasetDirectory, histogramsDirectory, partitionMatrixDirectory);
			} else if (argsMap.get("executionMode").equals("rearrangement")) {
				/*	Input	*/
				String rearrangementPolicy = argsMap.get("rearrangementPolicy");
				int numPartitions = Integer.valueOf(argsMap.get("numPartitions"));
				
				Path datasetDirectory = new Path(argsMap.get("datasetDirectory"));
				Path partitionMatrixDirectory = new Path(argsMap.get("partitionMatrixDirectory"));
				Path tspk = rearrangementPolicy.startsWith("TSPk")?new Path(argsMap.get("tspk")):new Path("/");
				
				/*	Output	*/
				Path partitionMatrixRearrangedDirectory = new Path(argsMap.get("partitionMatrixRearrangedDirectory"));

				/*	Execution	*/
				ctrl.executeRearrangement(rearrangementPolicy,numPartitions, datasetDirectory,
					partitionMatrixDirectory, partitionMatrixRearrangedDirectory, tspk);
			} else if (argsMap.get("executionMode").equals("partitioning")) {
				/*	Input	*/
				int numPartitions = Integer.valueOf(argsMap.get("numPartitions"));
				String partitioningPolicy = argsMap.get("partitioningPolicy");
				SearchPolicy sp = SearchPolicy.BINARY_SEARCH;
				if(argsMap.containsKey("searchPolicy")) {
					sp = SearchPolicy.valueOf(argsMap.get("searchPolicy"));
				}
				String rangeSearchWeights = null;
				if (argsMap.containsKey("rangeSearchWeights")) {
					rangeSearchWeights = argsMap.get("rangeSearchWeights");
				}
				
				String rangeSearchUpperBoundGranularity = null;
				if (argsMap.containsKey("rangeSearchUpperBoundGranularity")) {
					rangeSearchUpperBoundGranularity = argsMap.get("rangeSearchUpperBoundGranularity");
				}
				
				BinarySearchPolicy bsp = BinarySearchPolicy.MAX_PARTITION_INPUT;
				if(argsMap.containsKey("binarySearchPolicy")) {
					bsp = BinarySearchPolicy.valueOf(argsMap.get("binarySearchPolicy"));
				}
				
				Path datasetDirectory = new Path(argsMap.get("datasetDirectory"));
				Path rearrangements = new Path(File.separator);
				if(argsMap.containsKey("rearrangements")) {
					rearrangements = new Path(argsMap.get("rearrangements"));
				}
				Path partitionMatrixDirectory = new Path(argsMap.get("partitionMatrixDirectory"));
				
				Path defaultPartitioningDirectory = null;
				if(argsMap.containsKey("defaultPartitioningDirectory")) {
					defaultPartitioningDirectory = new Path(argsMap.get("defaultPartitioningDirectory"));
				}
				
				/*	Output	*/
				Path partitioningDirectory = new Path(argsMap.get("partitioningDirectory"));
				
				/*	Execution	*/
				new Controller().executePartitioning(numPartitions, partitioningPolicy, sp, rangeSearchUpperBoundGranularity, rangeSearchWeights, 
						bsp, datasetDirectory, rearrangements, defaultPartitioningDirectory,	partitionMatrixDirectory, partitioningDirectory);
			} else if (argsMap.get("executionMode").equals("join")) {
				/*	Input	*/
				int numPartitions = Integer.valueOf(argsMap.get("numPartitions"));
				int jobMaxExecutionHours = Integer.valueOf(argsMap.get("jobMaxExecutionHours"));
				
				Path datasetDirectory = new Path(argsMap.get("datasetDirectory"));
				Path rearrangements = new Path(File.separator);
				if(argsMap.containsKey("rearrangements")) {
					rearrangements = new Path(argsMap.get("rearrangements"));
				}
				Path properties = new Path(argsMap.get("properties"));
				Path histogramIndexToPartitionsMapping = new Path(argsMap.get("histogramIndexToPartitionsMapping"));
				Path partitionToCellsMapping = new Path(argsMap.get("partitionToCellsMapping"));
				
				/*	Output	*/
				Path mBucketIDirectory = new Path(argsMap.get("mBucketIDirectory"));
				
				/*	Execution	*/
				new Controller().executeMBucketI(numPartitions,jobMaxExecutionHours,datasetDirectory,rearrangements,
					properties,histogramIndexToPartitionsMapping,partitionToCellsMapping,mBucketIDirectory);
			} else if (argsMap.get("executionMode").equals("virtualPartitionMatrix")) {
				/**
				 * @deprecated
				 * 
				 * This functionality supports the creation of an old idea, where creation
				 * of PartitionMatrices that did not correspond to actual data was possible.
				 * 
				 * TODO: delete this mode, as AD-HOC queries can be emulated with synthetic data.
				 */
				/*	Input	*/
				int buckets = Integer.valueOf(argsMap.get("buckets"));
				int sparsity = Integer.valueOf(argsMap.get("sparsity"));
				int bands = Integer.valueOf(argsMap.get("bands"));
				int bandsOffsetSeed = Integer.valueOf(argsMap.get("bandsOffsetSeed"));
				String bandType = argsMap.get("bandType");
				
				/*	Output	*/
				Path partitionMatrixDirectory = new Path(argsMap.get("partitionMatrixDirectory"));

				/*	Execution	*/
				new Controller().executeVirtualPartitionMatrix(buckets, sparsity, bands, 
					bandsOffsetSeed,bandType,partitionMatrixDirectory);
			}
		} catch (IOException e) {
			System.err.println("IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (RearrangementError e) {
			System.err.println("RearrangementError");
			e.printStackTrace();
		} catch (PartitioningError e) {
			System.err.println("PartitioningError: " + e.getMessage());
			e.printStackTrace();
		} catch (JoinException e) {
			System.err.println("JoinException: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	/*
	 * 	Part 2 - Execution mode methods
	 * 
	 *	Each one of these methods implements a different execution mode of the system.
	 */
	
	private void executeDataGeneration(long from, long to, long valueReplication, long sizeS, 
			long sizeT, long generatorSeed,String generatorMode, String distribution, 
			Path datasetDirectory) throws IOException {
		
		Path dataset = new Path(datasetDirectory + File.separator + "files" + File.separator + "dataset.csv");
		Path datasetSizes = new Path(datasetDirectory + File.separator + "datasetSizes.csv");
		Path executionTimes = new Path(datasetDirectory + File.separator + "executionTimes.csv");
		
		DatasetGeneratorExporter dge = new DatasetGeneratorExporter(generatorSeed, dataset, datasetSizes);
		if (generatorMode.equals("range")) {
			dge.generateRange(from,to,valueReplication);
		} else if (generatorMode.equals("distribution")) {
			dge.generateDistribution(from, to, sizeS, sizeT, distribution);
		}
		dge.exportExecutionTimes(executionTimes);
	}
	
	private void executeHistograms(int buckets, String samplingRatio, Path dataset, Path datasetSizes, Path histogramsDirectory) throws IOException  {
		Path boundaries = new Path(histogramsDirectory + File.separator + "boundaries.csv");
		Path counts = new Path(histogramsDirectory + File.separator + "counts.csv");
		Path executionTimes = new Path(histogramsDirectory + File.separator + "executionTimes.csv");
		
		long sizeS = new DatasetImporter().getSize("S", datasetSizes);
		long sizeT = new DatasetImporter().getSize("T", datasetSizes);
		
		Path datasetFiles = new Path(dataset + File.separator + "files");
		
		HistogramCreator hc = new HistogramCreator();
		hc.createHistogram(sizeS,sizeT,samplingRatio,buckets,datasetFiles,boundaries,counts);
		
		new HistogramsExporter().exportExecutionTimes(hc.getExecutionTimeEquiDepthHistograms1(),
				hc.getExecutionTimeEquiDepthHistograms2(), executionTimes);
	}

	private void executePartitionMatrix(int buckets, int sparsity, int bands, int bandsOffsetSeed,
		Path datasetDirectory, Path histogramsDirectory, Path partitionMatrixDirectory) throws IOException {
		
		Path dataset = new Path(datasetDirectory + File.separator + "files");
		Path datasetSizes = new Path(datasetDirectory + File.separator + "datasetSizes.csv");
		
		Path boundaries = new Path(histogramsDirectory + File.separator + "boundaries.csv");
		Path counts = new Path(histogramsDirectory + File.separator + "counts.csv");
		
		long sizeS = new DatasetImporter().getSize("S", datasetSizes);
		long sizeT = new DatasetImporter().getSize("T", datasetSizes);
		
		BucketBoundaries[] boundariesS = new HistogramImporter().importBoundaries("S", buckets, boundaries);
		BucketBoundaries[] boundariesT = new HistogramImporter().importBoundaries("T", buckets, boundaries);
		
		long[] countsS = new HistogramImporter().importCounts("S", buckets, counts);
		long[] countsT = new HistogramImporter().importCounts("T", buckets, counts);
		
		PartitionMatrixFactory pmf = new PartitionMatrixFactory(buckets,boundariesS,boundariesT,
																countsS,countsT,sizeS,sizeT,
																bands,sparsity,bandsOffsetSeed);
		PartitionMatrix pm = pmf.getPartitionMatrix();
		
		PartitionMatrixExporter pme = new PartitionMatrixExporter(pm,partitionMatrixDirectory);
		
		Path partitionMatrix = new Path(partitionMatrixDirectory + File.separator + "pm.csv");
		//Path partitionMatrixBoundaries = new Path(partitionMatrixDirectory + File.separator + "boundaries.csv");
		//Path partitionMatrixCounts = new Path(partitionMatrixDirectory + File.separator + "counts.csv");
		Path properties = new Path(partitionMatrixDirectory + File.separator + "properties.csv");
		Path partitionMatrixPNG = new Path(partitionMatrixDirectory + File.separator + "pm.png");
		Path executionTimes = new Path(partitionMatrixDirectory + File.separator + "executionTimes.csv");
		
		pme.exportPartitionMatrixCSV(partitionMatrix);
		//pme.exportBoundariesCSV(partitionMatrixBoundaries);
		//pme.exportCountsCSV(partitionMatrixCounts);
		
		String query = PartitionMatrixFactory.getQuery(bands,sparsity,bandsOffsetSeed,buckets,boundariesS,boundariesT);
		
		pme.exportPropertiesCSV(sizeS,sizeT,buckets,sparsity,bands,bandsOffsetSeed,query,dataset, properties);
		pme.exportPartitionMatrixPNG(buckets,sparsity,bands,bandsOffsetSeed, dataset, partitionMatrixPNG);
		pme.exportExecutionTimes(pmf.getExecutionTime(), executionTimes);
	}
	
	private void executeRearrangement(String rearrangementPolicy, int numPartitions, Path datasetDirectory, Path partitionMatrixDirectory, 
		Path partitionMatrixRearrangedDirectory, Path tspk) throws IOException, RearrangementError {
		Path partitionMatrix = new Path(partitionMatrixDirectory.toString() + File.separator + "pm.csv");
		
		Path properties = new Path(partitionMatrixDirectory.toString() + File.separator + "properties.csv");
		
		Path partitionMatrixRearranged = new Path(partitionMatrixRearrangedDirectory.toString() + File.separator + "pm.csv");
		Path propertiesRearranged = new Path(partitionMatrixRearrangedDirectory.toString() + File.separator + "properties.csv");
		Path partitionMatrixRearrangedPNG = new Path(partitionMatrixRearrangedDirectory.toString() + File.separator + "pm.png");
		Path rearrangements = new Path(partitionMatrixRearrangedDirectory.toString() + File.separator + "rearrangements.csv");
		Path executionTimes = new Path(partitionMatrixRearrangedDirectory.toString() + File.separator + "executionTimes.csv");
		
		PartitionMatrixImporter pmi = new PartitionMatrixImporter();
		
		long sizeS = Long.valueOf(pmi.importProperty("sizeS", properties));
		long sizeT = Long.valueOf(pmi.importProperty("sizeT", properties));
		int buckets = Integer.valueOf(pmi.importProperty("buckets", properties));
		
		Path boundaries = new Path(datasetDirectory + File.separator + String.valueOf(buckets) + File.separator + "boundaries.csv");
		Path counts = new Path(datasetDirectory + File.separator + String.valueOf(buckets) + File.separator + "counts.csv");
		
		PartitionMatrix pm = pmi.importPartitionMatrix(sizeS,sizeT,buckets,partitionMatrix,new Path(File.separator),boundaries,counts);
		
		Rearrangements r = null;
		if (rearrangementPolicy.equals("BEA")) {
			r = new BondEnergyAlgorithm(pm);
		} else if (rearrangementPolicy.equals("BEARadius")) {
			r = new BondEnergyAlgorithmRadius(pm, 3); // XXX: default radius 3! Could be parameterized.
		} else if (rearrangementPolicy.equals("TSPk")) {
			r = new TSPAlgorithm(pm, numPartitions, buckets,rearrangementPolicy,partitionMatrixDirectory);
		} else if (rearrangementPolicy.equals("TSPkTransposedOnDefault")) {
			r = new TSPAlgorithmTransposedOnDefault(pm, numPartitions, buckets,partitionMatrixDirectory);
		} else if (rearrangementPolicy.equals("TSPkTransposedOnRearranged")) {
			r = new TSPAlgorithmTransposedOnRearranged(pm, numPartitions, buckets,partitionMatrixDirectory);
		}
		r.rearrange();
			
		PartitionMatrixExporter pme = new PartitionMatrixExporter(pm,partitionMatrixRearrangedDirectory);
		
		pme.exportPartitionMatrixCSV(partitionMatrixRearranged);
		pme.exportProperties(pmi.importProperties(properties),propertiesRearranged);
		
		String currentRearrangements = pmi.importProperty("rearrangements", propertiesRearranged);
		currentRearrangements = (currentRearrangements.equals("")?
				rearrangementPolicy:currentRearrangements + "_" + rearrangementPolicy);
		pme.exportProperty("rearrangements", currentRearrangements, propertiesRearranged);
		
		/*	XXX: Export property dependent on rearrangementPolicy.	*/
		if (rearrangementPolicy.startsWith("TSPk")) {
			pme.exportProperty("numPartitions", String.valueOf(numPartitions), propertiesRearranged);
		} else if (rearrangementPolicy.equals("BEARadius")) {
			pme.exportProperty("beaRadius", "3", propertiesRearranged);
		}
		
		RearrangementsExporter re = new RearrangementsExporter(partitionMatrixRearrangedDirectory);
		re.exportRearrangementsCSV(r.getRowsRearrangements(), r.getColumnsRearrangements(), rearrangements);
		re.exportExecutionTimes(r.getExecutionTime(), executionTimes);
		
		int sparsity = Integer.valueOf(pmi.importProperty("sparsity", properties));
		int bands = Integer.valueOf(pmi.importProperty("bands", properties));
		int bandsOffsetSeed = Integer.valueOf(pmi.importProperty("bandsOffsetSeed", properties));
		Path dataset = new Path(pmi.importProperty("dataset", properties));
		
		re.exportPartitionMatrixPNG(pm, buckets, sparsity, bands, bandsOffsetSeed, 
				rearrangementPolicy, numPartitions, 3, dataset, partitionMatrixRearrangedPNG);
	}
	
	/**
	 * 
	 * @param numPartitions
	 * @param partitioningPolicy
	 * @param maxPartitionInputPolicy
	 * @param defaultPartitioning: this is only given in case "GIVEN" is the value of maxPartitionInputPolicy
	 * @param datasetDirectory
	 * @param rearrangements
	 * @param partitionMatrixDirectory
	 * @param partitioningDirectory
	 * @throws IOException
	 * @throws PartitioningError
	 */
	private void executePartitioning(int numPartitions, String partitioningPolicy, SearchPolicy sp, String rangeSearchUpperBoundGranularity, String rangeSearchWeights,
			BinarySearchPolicy bsp, Path datasetDirectory, Path rearrangements, Path defaultPartitioningDirectory, Path partitionMatrixDirectory, Path partitioningDirectory) throws IOException, PartitioningError {
		
		Path partitionMatrix = new Path(partitionMatrixDirectory.toString() + File.separator + "pm.csv");
		Path properties = new Path(partitionMatrixDirectory.toString() + File.separator + "properties.csv");
		
		PartitionMatrixImporter pmi = new PartitionMatrixImporter();
		
		long sizeS = Long.valueOf(pmi.importProperty("sizeS", properties));
		long sizeT = Long.valueOf(pmi.importProperty("sizeT", properties));
		int buckets = Integer.valueOf(pmi.importProperty("buckets", properties));

		Path boundaries = new Path(datasetDirectory + File.separator + String.valueOf(buckets) + File.separator + "boundaries.csv");
		Path counts = new Path(datasetDirectory + File.separator + String.valueOf(buckets) + File.separator + "counts.csv");
		
		long[] countsS = new HistogramImporter().importCounts("S",buckets,counts);
		
		PartitionMatrix pm = pmi.importPartitionMatrix(sizeS,sizeT,buckets,partitionMatrix, rearrangements,boundaries,counts);
		
		long maxIC = Long.MIN_VALUE;
		long maxCC = Long.MIN_VALUE;
		
		HashMap<String, String> dmParameters = new HashMap<String, String>();

		Partitioner prt = null;
		
		DistanceMeasure dm = null;
		if (partitioningPolicy.equals("AICPM")) {
			dm = DistanceMeasure.ADDED_INPUT_COST_PARTITION_MAX;
		} else if (partitioningPolicy.equals("ACCPM")) {
			dm = DistanceMeasure.ADDED_CANDIDATE_CELLS_PARTITION_MAX;
		} else if (partitioningPolicy.equals("AICPS")) {
			dm = DistanceMeasure.ADDED_INPUT_COST_PARTITION_SUM;
		} else if (partitioningPolicy.equals("EPIC")) {
			dm = DistanceMeasure.EMPTIEST_PARTITION_INPUT_COST;
		} else if (partitioningPolicy.equals("EPCC")) {
			dm = DistanceMeasure.EMPTIEST_PARTITION_CANDIDATE_CELLS;
		} else if (partitioningPolicy.equals("JB")) {
			dm = DistanceMeasure.JACCARD_BUCKETS;
		} else if (partitioningPolicy.startsWith("WICRC")) {
			dm = DistanceMeasure.WEIGHTED_INPUT_COST_ROWS_COLUMNS;
			String[] toks = partitioningPolicy.split("_");
			dmParameters.put("wR", toks[1]);
			dmParameters.put("wC", toks[2]);
		} else if (partitioningPolicy.equals("M")) {
			dm = DistanceMeasure.MANHATTAN;
		} else if (partitioningPolicy.equals("MBI") || partitioningPolicy.equals("MBIREP")) {
			;// Do nothing...
		} else if (partitioningPolicy.equals("targetf")) {
			; // Do Nothing
		} else {
			String errorMessage = 	"ERROR - Controller.executePartitioning, No options for "+
									"Merging partition to partition passed.";
			throw new PartitioningError(errorMessage);
		}
		
		/* Experimental merging by target function */
		if(partitioningPolicy.equals("targetf")) {
			prt = new MergingPartitionToPartitionF(pm, partitioningPolicy, sizeS, sizeT, numPartitions);
			prt.execute(-1);
			
			HashMap<String, Double> metricsCase = new AllMetrics(prt.getPartitions(),prt.getPartitionsCount(),sizeS,sizeT,buckets,buckets, countsS).getAllMetrics();
			
			exportPartitioning(prt, pm, pmi, numPartitions, partitioningPolicy, sizeS, sizeT, buckets,metricsCase,  properties, partitioningDirectory);
		} else if (sp == Partitioner.SearchPolicy.BINARY_SEARCH) {
			if (partitioningPolicy.equals("MBI") || partitioningPolicy.equals("MBIREP")) { // TODO: remove partitioningPolicy: MBIREP
				prt = new MBIPartitioner(pm,partitioningPolicy,sizeS,sizeT,numPartitions);
			} else {
				prt = new MergingPartitionToPartition(pm,partitioningPolicy,sizeS,sizeT,numPartitions,dm,dmParameters, bsp);
			}
			prt.findLowest();
			
			HashMap<String, Double> metricsCase = new AllMetrics(prt.getPartitions(),prt.getPartitionsCount(),sizeS,sizeT,buckets,buckets, countsS).getAllMetrics();
			
			exportPartitioning(prt, pm, pmi, numPartitions, partitioningPolicy, sizeS, sizeT, buckets,metricsCase,  properties, partitioningDirectory);
		} else if (sp == Partitioner.SearchPolicy.RANGE_SEARCH) {				
			if (partitioningPolicy.equals("MBI") || partitioningPolicy.equals("MBIREP")) { // TODO: remove partitioningPolicy: MBIREP
				prt = new MBIPartitioner(pm,partitioningPolicy,sizeS,sizeT,numPartitions);
			} else {
				prt = new MergingPartitionToPartition(pm,partitioningPolicy,sizeS,sizeT,numPartitions,dm, dmParameters, bsp);
			}
			
			Path defaultPartitioningProperties = new Path(defaultPartitioningDirectory.toString() + File.separator + "partitionsStatistics.csv");
			
			maxIC = (long) Double.parseDouble(pmi.importProperty("BJmaxIC", defaultPartitioningProperties));
			maxCC = (long) Double.parseDouble(pmi.importProperty("BJmaxCC", defaultPartitioningProperties));
			
			String[] ubgToks = rangeSearchUpperBoundGranularity.split("_");
			Double upperBound = Double.valueOf(ubgToks[0]);
			Integer granularity = Integer.valueOf(ubgToks[1]);
						
			long lower = Long.MIN_VALUE;
			long upper = Long.MIN_VALUE;
			switch(bsp) {
				case MAX_PARTITION_INPUT:
					lower = maxIC;
					upper = (long) (upperBound * maxIC);
					break;
				case MAX_PARTITION_CANDIDATE_CELLS:
					lower = maxCC;
					upper = (long) (upperBound * maxCC);
					break;
			}
			
			prt.rangeSearch(lower, upper, granularity);
			
			PartitionerRangeSearchHandler prsh = new PartitionerRangeSearchHandler();
			
			String[] weightsToks = rangeSearchWeights.split("\\|");
			for (String weightsTok : weightsToks) {
				TreeMap<String, Double> weights = new TreeMap<String, Double>();
				
				String[] weightsMetrics = weightsTok.split("-");
				for(String weight : weightsMetrics) {
					String[] weightToks = weight.split("_");
					
					weights.put(weightToks[0], Double.valueOf(weightToks[1]));
				}
				
				prsh.calculateBestCaseRangeSearch(prt, weights);
				
				switch(bsp) {
					case MAX_PARTITION_INPUT: // The best maxIC
						long best_maxIC = prt.getRangeSearchMetrics().get(0).getMetrics().get("BJmaxIC").longValue();
						prt.execute(best_maxIC);
						break;
					case MAX_PARTITION_CANDIDATE_CELLS: // The best maxCC
						long best_maxCC = prt.getRangeSearchMetrics().get(0).getMetrics().get("BJmaxCC").longValue();
						prt.execute(best_maxCC);
						break;
				}
				
				HashMap<String, Double> metricsBestCase = new AllMetrics(prt.getPartitions(),prt.getPartitionsCount(),sizeS, sizeT, buckets,buckets, countsS).getAllMetrics();
				
				Path partitioningWeightsDirectory = new Path(partitioningDirectory.toString() + File.separator + weightsTok);
				
				exportPartitioning(prt, pm, pmi, numPartitions, partitioningPolicy, sizeS, sizeT, buckets, metricsBestCase, properties, partitioningWeightsDirectory);
				
				Path rangeSearchMetricsCSV = new Path(partitioningWeightsDirectory.toString() + File.separator + "rangeSearchMetrics.csv");
				Path rangeSearchMetricsNormalizedCSV = new Path(partitioningWeightsDirectory.toString() + File.separator + "rangeSearchMetricsNormalized.csv");
				Path rangeSearchMetricsNormalizedXYChartPNG = new Path(partitioningWeightsDirectory.toString() + File.separator + "rangeSearchMetricsNormalized.png");
						
				PartitioningExporter pe = new PartitioningExporter(pm,partitioningDirectory);
				
				pe.exportRangeSearchMetricsCSV(prt.getRangeSearchMetrics(), rangeSearchMetricsCSV);
				pe.exportRangeSearchMetricsNormalizedCSV(prt.getRangeSearchMetrics(), AllMetrics.getMaxMetrics(prt.getRangeSearchMetrics()), prsh.getMaxScore(), rangeSearchMetricsNormalizedCSV);
				pe.exportRangeSearchMetricsNormalizedPNG(prt.getRangeSearchMetrics(), AllMetrics.getMaxMetrics(prt.getRangeSearchMetrics()), prsh.getMaxScore(), rangeSearchMetricsNormalizedXYChartPNG);
			}
		}
	}
	
	private void exportPartitioning(Partitioner prt, PartitionMatrix pm, PartitionMatrixImporter pmi, 
									int numPartitions, String partitioningPolicy, long sizeS, long sizeT,
									 int buckets, HashMap<String, Double> metricsCase, Path properties,
									 Path partitioningDirectory) throws IOException {
		
		Partition[] partitions = prt.getPartitions();
		
		long executionTimeBinarySearch = prt.getExecutionTimeBinarySearch();
		long executionTimeRangeSearch = prt.getExecutionTimeRangeSearch();
		
		long[] partitionsInputCosts = new long[prt.getPartitionsCount()];
		int partitionsCounter = 0;
		for (Partition partition: partitions) {
			if (partition == null) {
				continue;
			}
			partitionsInputCosts[partitionsCounter++] = partition.computeInputCost();
		}
		
		Path histogramIndexToPartitionsMapping = new Path(partitioningDirectory.toString() + File.separator + "histogramIndexToPartitionsMapping.csv");
		Path partitionsInputCost = new Path(partitioningDirectory.toString() + File.separator + "partitionsInputCost.csv");
		Path partitionsStatistics = new Path(partitioningDirectory.toString() + File.separator + "partitionsStatistics.csv");
		Path partitionToCellsMapping = new Path(partitioningDirectory.toString() + File.separator + "partitionToCellsMapping.csv");
//		Path partitionToPartitionDistance = new Path(partitioningDirectory.toString() + File.separator + "partitionToPartitionDistance.csv");
//		Path partitionToIndicesMapping = new Path(partitioningDirectory.toString() + File.separator + "partitionToIndicesMapping.csv");
		
		PartitioningExporter pe = new PartitioningExporter(pm,partitioningDirectory);
		
		Path partitionRectangularBoundaries = new Path(partitioningDirectory.toString() + File.separator + "partitionRectangularBoundaries.csv");
		Path partitionMatrixRectangularPNG = new Path(partitioningDirectory.toString() + File.separator + "partitionMatrixRectangular.png");
		Path executionTimes = new Path(partitioningDirectory.toString() + File.separator + "executionTimes.csv");

		pe.exportPartitionToCellsMappingCSV(partitionToCellsMapping, partitions);
//		rse.exportPartitionToIndicesMappingCSV(partitionToIndicesMapping, partitions);
		pe.exportHistogramIndexToPartitionsMappingCSV(histogramIndexToPartitionsMapping,prt.getIdxToPartitionsS(),prt.getIdxToPartitionsT());
		pe.exportPartitionsInputCostCSV(partitionsInputCost,partitions);
		pe.exportPartitionsStatisticsCSV(sizeS, sizeT, buckets, buckets, partitionsCounter, metricsCase, partitionsStatistics);
		
		if(partitioningPolicy.equals("MBI") || partitioningPolicy.equals("MBIREP")) {
			pe.exportPartitionRectangularBoundariesCSV(partitionRectangularBoundaries, (PartitionRectangular[]) partitions);
		} 
//		else if(partitioningPolicy.equals("AICGM") || partitioningPolicy.equals("AICGS")) {
//			rse.exportPartitionToPartitionDistanceCSV(partitionToPartitionDistance, ((MergingPartitionToPartition)prt).getDistancePairs());
//		}
		
		int sparsity = Integer.valueOf(pmi.importProperty("sparsity", properties));
		int bands = Integer.valueOf(pmi.importProperty("bands", properties));
		int bandsOffsetSeed = Integer.valueOf(pmi.importProperty("bandsOffsetSeed", properties));
		String rearrangementsProperty = pmi.importProperty("rearrangements",properties);
		
		int beaRadius = -1;
		if (rearrangementsProperty.endsWith("BEARadius")) {
			beaRadius = Integer.valueOf(pmi.importProperty("beaRadius", properties));
		}

		if (partitioningPolicy.equals("MBI") || partitioningPolicy.equals("MBIREP")) {
			pe.exportPartitionMatrixRectangularPNG(buckets,sparsity,bands,bandsOffsetSeed,rearrangementsProperty,
					partitioningPolicy,numPartitions, beaRadius, metricsCase,partitionMatrixRectangularPNG,(PartitionRectangular[])partitions);
		}
		pe.exportExecutionTimes(executionTimeBinarySearch, executionTimeRangeSearch, executionTimes);
	}

	private void executeMBucketI(int numPartitions,int jobMaxExecutionHours, Path datasetDirectory, Path rearrangements, Path properties, Path histogramIndexToPartitionsMapping,
			Path partitionToCellsMapping,	Path mBucketIDirectory) throws IOException, JoinException {
		PartitionMatrixImporter pmi = new PartitionMatrixImporter();
		long sizeS = Long.valueOf(pmi.importProperty("sizeS", properties));
		long sizeT = Long.valueOf(pmi.importProperty("sizeT", properties));
		int buckets = Integer.valueOf(pmi.importProperty("buckets", properties));
		String query = pmi.importProperty("query",properties);
		
		Path dataset = new Path(datasetDirectory + File.separator + "files");
		
		Path boundaries = new Path(datasetDirectory + File.separator + String.valueOf(buckets) + File.separator + "boundaries.csv");
		
		MBucketI mbi = new MBucketI(buckets,query,numPartitions,jobMaxExecutionHours,dataset,boundaries,rearrangements,histogramIndexToPartitionsMapping, partitionToCellsMapping);
		mbi.join();
		
		MBucketIExporter mbie = new MBucketIExporter();
		
		Path counters = new Path(mBucketIDirectory + File.separator + "counters.csv");
		Path partitionsStatistics = new Path(mBucketIDirectory + File.separator + "partitionsStatistics.csv");
		Path executionTimes = new Path(mBucketIDirectory + File.separator + "executionTimes.csv");
		
		mbie.exportCountersCSV(mbi.getCountersMBucketIPartitionsInOut(), counters);
		mbie.exportMBucketIStatisticsCSV(sizeS, sizeT, mbi.getCountersMBucketIPartitionsInOut(), partitionsStatistics);
		mbie.exportExecutionTimes(mbi.getExecutionTime(), executionTimes);
		
	}
	
	/**
	 * @deprecated
	 * 
	 * This functionality supports the creation of an old idea, where creation
	 * of PartitionMatrices that did not correspond to actual data was possible.
	 * 
	 * TODO: delete this mode, as AD-HOC queries can be emulated with synthetic data.
	 */
	private void executeVirtualPartitionMatrix(int buckets, int sparsity, int bands, 
			int bandsOffsetSeed, String bandType, Path partitionMatrixDirectory) throws IOException {
		
		int sizeS = buckets;
		int sizeT = buckets;
		
		VirtualPartitionMatrixFactory vpmf = new VirtualPartitionMatrixFactory(buckets,sparsity,bands,bandsOffsetSeed,bandType);
		PartitionMatrix pm = vpmf.getPartitionMatrix();
		
		PartitionMatrixExporter pme = new PartitionMatrixExporter(pm,partitionMatrixDirectory);
		
		Path partitionMatrix = new Path(partitionMatrixDirectory + File.separator + "pm.csv");
		Path partitionMatrixBoundaries = new Path(partitionMatrixDirectory + File.separator + "boundaries.csv");
		Path partitionMatrixCounts = new Path(partitionMatrixDirectory + File.separator + "counts.csv");
		Path properties = new Path(partitionMatrixDirectory + File.separator + "properties.csv");
//		Path partitionMatrixEPS = new Path(partitionMatrixDirectory + File.separator + "pm.eps");
		Path partitionMatrixPNG = new Path(partitionMatrixDirectory + File.separator + "pm.png");
		Path executionTimes = new Path(partitionMatrixDirectory + File.separator + "executionTimes.csv");
		
		pme.exportPartitionMatrixCSV(partitionMatrix);
		pme.exportBoundariesCSV(partitionMatrixBoundaries);
		pme.exportCountsCSV(partitionMatrixCounts);
		
		String query = "no_query";
		
		pme.exportPropertiesCSV(sizeS,sizeT,buckets,sparsity,bands,bandsOffsetSeed,query,new Path("virtualMatrix"), properties);
//		pme.exportPartitionMatrixEPS(partitionMatrixEPS);
		pme.exportPartitionMatrixPNG(buckets,sparsity,bands,bandsOffsetSeed, new Path("virtualMatrix"), partitionMatrixPNG);
		pme.exportExecutionTimes(vpmf.getExecutionTime(), executionTimes);
	}
	
}
