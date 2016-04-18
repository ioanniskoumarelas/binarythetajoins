/**
 * MBucketI.java
 * 
 * This class implements the join process in a MapReduce environment,
 * as described at:
 * 
 * "Processing theta-joins using MapReduce"
 * 
 * under the method name MBucketI.
 * 
 * @author John Koumarelas
 */

package join.mbi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import join.ThetaJoin;
import model.BucketBoundaries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.importers.HistogramImporter;
import utils.importers.PartitioningImporter;
import utils.importers.RearrangementsImporter;
import datatypes.LongPair;
import datatypes.LongTriple;
import datatypes.exceptions.JoinException;

public class MBucketI extends ThetaJoin {

	private int buckets;
	private String query;
	private int numReducers;
	private int jobMaxExecutionHours;
	private Path dataset;
	private Path boundaries;
	private Path rearrangements;
	private Path histogramIndexToPartitionsMapping;
	private Path partitionToCellsMapping;
	
	private final static Path outputMBucketI = new Path("btj/mbi");
	
	private long executionTime;
	
	private HashMap<String, LongTriple> countersMBucketIPartitionsInOut = new HashMap<String,LongTriple>();

	public MBucketI(int buckets, String query, int numPartitions, int jobMaxExecutionHours, Path dataset,Path boundaries,Path rearrangements, Path histogramIndexToPartitionsMapping, Path groupToCellMapping) {
		this.buckets = buckets;
		this.query = query;
		this.numReducers = numPartitions;
		this.jobMaxExecutionHours = jobMaxExecutionHours;
		this.dataset = dataset;
		this.boundaries = boundaries;
		this.rearrangements = rearrangements;
		this.histogramIndexToPartitionsMapping = histogramIndexToPartitionsMapping;
		this.partitionToCellsMapping = partitionToCellsMapping;
	}

	public static class MBIMapper extends
	Mapper<LongWritable, Text, IntPair, LongWritable> {

		private BucketBoundaries[] boundariesS;
		private BucketBoundaries[] boundariesT;

		private int[] defaultToRearrangedS = null;
		private int[] defaultToRearrangedT = null;
		
		private HashMap<Integer,ArrayList<Integer>> hmS;
		private HashMap<Integer,ArrayList<Integer>> hmT;
		
		private final int relID_S = 0;
		private final int relID_T = 1;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			// Boundaries
			int buckets = Integer.valueOf(conf.get("buckets"));
			Path boundaries = new Path(conf.get("boundaries"));
			
			boundariesS = new HistogramImporter().importBoundaries("S", buckets, boundaries);
			boundariesT = new HistogramImporter().importBoundaries("T", buckets, boundaries);
			
			// Rearrangements
			if(!conf.get("rearrangements").equals("/")) {
				Path rearrangements = new Path(conf.get("rearrangements"));
				
				defaultToRearrangedS = new RearrangementsImporter().importDefaultToRearranged("S", buckets, rearrangements);
				defaultToRearrangedT = new RearrangementsImporter().importDefaultToRearranged("T", buckets, rearrangements);
			} else {
				defaultToRearrangedS = new int[buckets];
				defaultToRearrangedT = new int[buckets];
				
				for(int i = 0 ; i < buckets; ++i) {
					defaultToRearrangedS[i] = i;
					defaultToRearrangedT[i] = i;
				}
			}
			
			// Mapping
			Path histogramIndexToPartitionsMapping = new Path(conf.get("histogramIndexToPartitionsMapping"));
			
			hmS = new HashMap<Integer,ArrayList<Integer>>();
			hmT = new HashMap<Integer,ArrayList<Integer>>();
			new PartitioningImporter().importHistogramIndexToPartitionsMapping(hmS, hmT, histogramIndexToPartitionsMapping);
		}
		
		public int valueToBoundaryIndex(long value, boolean isRelationS) {
			
			BucketBoundaries[] boundaries = (isRelationS ? boundariesS : boundariesT);
			
			int start = 0;
		    int end = boundaries.length - 1;
		    int middle;
		    
		    while (start <= end) 
		    {
		        middle = start + (end - start)/2;
		        if (boundaries[middle].getFrom() <= value && value <= boundaries[middle].getTo()) {
		            return (isRelationS?defaultToRearrangedS:defaultToRearrangedT)[middle];
		        } else if (value < boundaries[middle].getFrom()) {
		            end = middle - 1;
		        } else if (boundaries[middle].getTo() < value) {
		        	start = middle + 1;
		        }
		    }
		    return -1;
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			if (line.trim().equals("")) {
				return;
			}
			
			String[] strToks = line.split(",");

			if (strToks != null) {				
				
				boolean isRelationS = strToks[0].equals("S");
				
				ArrayList<Integer> al = null;

				int histogramsIndex = -1;
				try {
					histogramsIndex = valueToBoundaryIndex(Integer.parseInt(strToks[1]), isRelationS);
					
					al = ( isRelationS ? hmS : hmT).get(histogramsIndex);
					
					// If not pruned by our algorithm, needs to be sent.
					if (al != null) {
						for (int i = 0 ; i < al.size(); ++i){
							IntPair ip = new IntPair();
							ip.set(al.get(i),isRelationS?relID_S:relID_T);
							
							context.write(ip,new LongWritable(Long.valueOf(strToks[1])));
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class MBIPartitioner extends Partitioner<IntPair,LongWritable> {
	    @Override
	    public int getPartition(IntPair key, LongWritable value, int numPartitions) {
	    	return key.getFirst()%numPartitions;
	    }
	}
	
	public static class MBIReducer extends Reducer<IntPair, LongWritable, Text, NullWritable> {
		private TreeMap<Long,Long> sValuesDuplication = new TreeMap<Long,Long>();
		private TreeMap<Long,Integer> sValuesToHistogramMapping = new TreeMap<Long, Integer>(); // Mapping of S values to their histogram index
		
		private HashMap<Integer,HashSet<datatypes.IntPair>> partitionToCellsMapping;
		
		private int buckets;
		private BucketBoundaries[] boundariesS;
		private BucketBoundaries[] boundariesT;
		
		private int[] defaultToRearrangedS;
		private int[] defaultToRearrangedT;
		
		private long counterInput = 0;
		
		private long counterOutput = 0;
		private long counterOutputCompletedLongMax = 0; // How many times counterOutput completed LONG_MAX
		
		private LongPair[] queryParts;
		
		//private long maxMemory;
		//private long memoryCounter;
		
		private final int relID_S = 0;
		private final int relID_T = 1;
		
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			
			queryParts = getQueryParts(context.getConfiguration().get("query"));
			//maxMemory = Runtime.getRuntime().maxMemory();
			//memoryCounter = 0;
			
			// Mapping
			partitionToCellsMapping = new PartitioningImporter().importPartitionToCellsMapping(new Path(conf.get("partitionToCellsMapping")));
			
			// Boundaries
			buckets = Integer.valueOf(conf.get("buckets"));
			Path boundaries = new Path(conf.get("boundaries"));
			
			boundariesS = new HistogramImporter().importBoundaries("S", buckets, boundaries);
			boundariesT = new HistogramImporter().importBoundaries("T", buckets, boundaries);
			
			// Rearrangements
			if(!conf.get("rearrangements").equals("/")) {
				Path rearrangements = new Path(conf.get("rearrangements"));
				
				defaultToRearrangedS = new RearrangementsImporter().importDefaultToRearranged("S", buckets, rearrangements);
				defaultToRearrangedT = new RearrangementsImporter().importDefaultToRearranged("T", buckets, rearrangements);
			} else {
				defaultToRearrangedS = new int[buckets];
				defaultToRearrangedT = new int[buckets];
				
				for(int i = 0 ; i < buckets; ++i) {
					defaultToRearrangedS[i] = i;
					defaultToRearrangedT[i] = i;
				}
			}
		}
		
		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
			
			String strID = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
			
			context.getCounter("reducerInput", strID).setValue(counterInput);
			context.getCounter("reducerOutput", strID).setValue(counterOutput);
			context.getCounter("reducerOutputCompletedLongMax", strID).setValue(counterOutputCompletedLongMax);
			
			super.cleanup(context);
		}
		
		public int valueToBoundaryIndex(long value, boolean isRelationS) {
			
			BucketBoundaries[] boundaries = (isRelationS ? boundariesS : boundariesT);
			
			int start = 0;
		    int end = boundaries.length - 1;
		     
		    while (start <= end) 
		    {
		        int middle = start + (end - start)/2;
		        if (boundaries[middle].getFrom() <= value && value <= boundaries[middle].getTo()) {
		            return (isRelationS?defaultToRearrangedS:defaultToRearrangedT)[middle];
		        } else if (value < boundaries[middle].getFrom()) {
		            end = middle - 1;
		        } else if (boundaries[middle].getTo() < value) {
		        	start = middle + 1;
		        }
		    }
		    return -1;
		}
		
		private static LongPair[] getQueryParts(String query) {
			String[] queryToks = query.trim().split("_");
			
			LongPair[] queryParts = new LongPair[queryToks.length];
			
			for (int i = 0 ; i < queryToks.length; ++i) {
				String[] toks = queryToks[i].substring(1,queryToks[i].length()-1).split("\\|");
				queryParts[i] = new LongPair(Integer.valueOf(toks[0]), Integer.valueOf(toks[1]));
			}
			
			return queryParts;
		}
		
		private void join(int key, long valueT, long duplicationT, int histogramIndexT, boolean[][] partitionCellsBitmap, Context context) 
				throws IOException, InterruptedException {
			//if( ((valueT + lp.getFirst()) <= valueS) && (valueS <= (valueT + lp.getSecond()) ) ) {
			
			for(LongPair lp : queryParts){
				
				long lowestValueT = valueT + lp.getFirst();
				long highestValueT = valueT + lp.getSecond();
				
				Long lowestS = sValuesDuplication.ceilingKey(lowestValueT);
				if (lowestS == null) {
					continue;
				}
				if (lowestS > highestValueT) {
					continue;
				}
				Long highestS = sValuesDuplication.floorKey(highestValueT);
				if (highestS == null) {
					continue;
				}
				if(highestS < lowestValueT) {
					continue;
				}
				 
				NavigableMap<Long,Long> possibleDuplicationS =  sValuesDuplication.subMap(lowestS, true, highestS, true);
				NavigableMap<Long,Integer> possibleHistogramS =  sValuesToHistogramMapping.subMap(lowestS, true, highestS, true);
				
				if(possibleDuplicationS.size() == 0) {
					continue;
				}
				
				Iterator<Entry<Long,Long>> itValuesSD = possibleDuplicationS.entrySet().iterator();
				Iterator<Entry<Long,Integer>> itValuesSHM = possibleHistogramS.entrySet().iterator();
				
				while(itValuesSD.hasNext()) {
					Entry<Long,Long> entrySD = itValuesSD.next();
					long valueS = entrySD.getKey();
					long duplicationS = entrySD.getValue();
					
					Entry<Long, Integer> entrySHM = itValuesSHM.next();
					int histogramIndexS = entrySHM.getValue();
					
					if(partitionCellsBitmap[histogramIndexS][histogramIndexT]) {
						if((Long.MAX_VALUE - counterOutput) <= (duplicationS*duplicationT)){
							++counterOutputCompletedLongMax;
							
							counterOutput = duplicationS*duplicationT - (Long.MAX_VALUE - counterOutput);
						} else {
							counterOutput += duplicationS*duplicationT;
						}
						
						String result;
						
						result = new String(valueS + "\t" + valueT);
						result += "\t"+duplicationS*duplicationT;
						
						context.write(new Text(result), NullWritable.get());
					}
				}
			}
		}
		
		private int previousRelation = -1;
		
		@Override
		protected void reduce(IntPair key, Iterable<LongWritable> values,
				Context context)
				throws IOException, InterruptedException {
			
			HashSet<datatypes.IntPair> partitionCells = partitionToCellsMapping.get(key.getFirst());
			boolean[][] partitionCellsBitmap = new boolean[buckets][buckets];
			for(int i = 0; i < buckets; ++i) {
				Arrays.fill(partitionCellsBitmap[i], false);
			}
			for(datatypes.IntPair pair: partitionCells) {
				partitionCellsBitmap[pair.getFirst()][pair.getSecond()] = true;
			}
			
			LongWritable value;
			
			if (key.getSecond() == relID_S){
				sValuesDuplication.clear();
				
				long valueS = -1;
				long duplicationS = 0;
				
				Iterator<LongWritable> itValues = values.iterator();
				while (itValues.hasNext()) {
					value = itValues.next();
					
					++counterInput;
					
					if (valueS == value.get()){
						++duplicationS;
					} else if (valueS == -1) {
						valueS = value.get();
						duplicationS = 1;
					} else { // the value has changed from a previous non default value
						if (sValuesDuplication.containsKey(valueS)) {
							duplicationS += sValuesDuplication.get(valueS);
						}
						
						sValuesDuplication.put(valueS,duplicationS);
						
						valueS = value.get();
						duplicationS = 1;
					}
				}
				/*	Last	*/
				if (valueS != -1){
					if (sValuesDuplication.containsKey(valueS)) {
						duplicationS += sValuesDuplication.get(valueS);
					} 
					sValuesDuplication.put(valueS,duplicationS);
				}
			} else {
				if (key.getSecond() == relID_T && previousRelation == relID_S){
					long valueT =-1;
					long duplicationT =0;
					
					boolean isRelationS = true;
					
					/*	Transform tuples from S to include their histogram indices.	*/
					sValuesToHistogramMapping.clear();
					for(Entry<Long,Long> entry : sValuesDuplication.entrySet()) {
						sValuesToHistogramMapping.put(entry.getKey(), valueToBoundaryIndex(entry.getKey(),isRelationS));
					}
					
					isRelationS = false;
					
					Iterator<LongWritable> itValues = values.iterator();
					while (itValues.hasNext()) {
						value = itValues.next();
						
						++counterInput;
						
						if(valueT == -1) {
							valueT = value.get();
							duplicationT = 1;
						} else if(valueT == value.get()) {
							++duplicationT;
						} else { // the value has changed from a previous non default value
							
							int histogramIndexT = valueToBoundaryIndex(valueT,isRelationS);
							
							join(key.getFirst(),valueT,duplicationT,histogramIndexT,partitionCellsBitmap, context);					
							valueT = value.get();
							duplicationT = 1;
						}
					}
					if(valueT != -1) {
						int histogramIndexT = valueToBoundaryIndex(valueT,isRelationS);
						join(key.getFirst(),valueT,duplicationT,histogramIndexT,partitionCellsBitmap, context);
					}
					
				}
			}
			previousRelation = key.getSecond();
		}
	}
	
	public void join() throws JoinException {
		try {
			Configuration conf = new Configuration();

			conf.set("mapred.task.timeout", String.valueOf(6*1000*1000*jobMaxExecutionHours));

			FileSystem fs = FileSystem.get(conf);

			
			Path currentOutputMBucketI = null;
			
			/* Consider using a distinct output path, when comparing results	*/
			//currentOutputMBucketI = new Path(outputMBucketI.toString()+"/"+histogramIndexToPartitionsMapping.getParent().toString());
			
			/*	Consider using a single output path, for storage space reasons	*/
			currentOutputMBucketI = outputMBucketI;
			
			fs.delete(currentOutputMBucketI,true);

			conf.set("buckets", String.valueOf(buckets));
			conf.set("query",query);
			conf.set("boundaries", boundaries.toString());
			conf.set("rearrangements",rearrangements.toString());
			conf.set("histogramIndexToPartitionsMapping", histogramIndexToPartitionsMapping.toString());
			conf.set("partitionToCellsMapping", partitionToCellsMapping.toString());

			Job job = new Job(conf, "MBucketI");
			
			System.out.println("Processing input path: " + dataset.toString());

			FileInputFormat.addInputPath(job, dataset);
			FileOutputFormat.setOutputPath(job,currentOutputMBucketI);

			job.setJarByClass(MBucketI.class);

			job.setMapperClass(MBIMapper.class);
			job.setPartitionerClass(MBIPartitioner.class);
			job.setReducerClass(MBIReducer.class);

			job.setMapOutputKeyClass(IntPair.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setNumReduceTasks(numReducers);

			long start = System.currentTimeMillis();
			int exitStatus = job.waitForCompletion(true) ? 0 : 1;
			long end = System.currentTimeMillis();
			
			executionTime = end-start;

			CounterGroup cgIn = job.getCounters().getGroup("reducerInput");
			CounterGroup cgOut = job.getCounters().getGroup("reducerOutput");
			CounterGroup cgOutCompletedLongMax = job.getCounters().getGroup("reducerOutputCompletedLongMax");

			countersMBucketIPartitionsInOut.clear();

			for (Counter counterIn : cgIn) {
				Counter counterOut = cgOut.findCounter(counterIn.getName());
				Counter counterOutCompletedLongMax = cgOutCompletedLongMax.findCounter(counterIn.getName());

				LongTriple lt = new LongTriple();
				lt.set(counterIn.getValue(), counterOutCompletedLongMax.getValue(), counterOut.getValue());
				countersMBucketIPartitionsInOut.put(counterIn.getName(), lt);
			}
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "ERROR - MBucketI#join(), Problem during the MapReduce join.";
			throw new JoinException(errorMessage);
		}
	}
	
	/*	Getters - Setters	*/
	
	public long getExecutionTime() {
		return executionTime;
	}
	
	public HashMap<String, LongTriple> getCountersMBucketIPartitionsInOut() {
		return countersMBucketIPartitionsInOut;
	}
	
}
