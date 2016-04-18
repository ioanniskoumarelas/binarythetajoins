/**
 * EquiDepthHistograms1Compressed.java
 * 
 * This class is responsible for the selection of the boundaries on the dataset
 * following the Equi-Depth technique.
 * 
 * It is compressed as it sends each value once from each relation, together
 * with a value which corresponds to its duplication/frequency, meaning how 
 * many times it exists in the actual dataset.
 * 
 * Results in: boundaries.csv
 * 
 * @author John Koumarelas
 */

package histogram.mapreduce;

import histogram.localExecution.DecideBoundariesCompressed;
import histogram.localExecution.DecideBoundariesCompressed.AssignmentResult;
import histogram.localExecution.DecideBoundariesCompressed.Bucket;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import datatypes.LongPair;
import datatypes.exceptions.HistogramsError;

public class EquiDepthHistograms1Compressed {
	
	private long sizeS;
	private long sizeT;
	private String samplingRatio;
	private int buckets;
	private Path dataset;
	private Path boundaries;
	
	private final static Path outputHistograms1 = new Path("btj/histograms1");
	
	private long executionTime;

	public EquiDepthHistograms1Compressed(long sizeS, long sizeT, String samplingRatio, int buckets, Path dataset, Path boundaries) {
		this.sizeS = sizeS;
		this.sizeT = sizeT;
		this.samplingRatio = samplingRatio;
		this.buckets = buckets;
		this.dataset = dataset;
		this.boundaries = boundaries;
	}

	public static class EDH1MapperCompressed extends Mapper<LongWritable, Text, IntWritable, LongPair> {
		private double probS;
		private double probT;
		
		private HashMap<Long,Long> valueReplicationS;
		private HashMap<Long,Long> valueReplicationT;
		
		private static final IntWritable keyS = new IntWritable(0);
		private static final IntWritable keyT = new IntWritable(1);
		
		private void getParameters(Context context) {
			
			long sizeS = Long.parseLong(context.getConfiguration().get("sizeS"));
			long sizeT = Long.parseLong(context.getConfiguration().get("sizeT"));
			
			String samplingRatio = context.getConfiguration().get("samplingRatio");
			if(samplingRatio.endsWith("%")) {
				String strN = samplingRatio.substring(0, samplingRatio.indexOf("%"));
				double ratio = Double.parseDouble(strN);
				probS = ratio / 100.0;
				probT = ratio / 100.0;
			} else {
				long n = Long.parseLong(context.getConfiguration().get("samplingRatio"));
				probS = n / (double) sizeS;
				probT = n / (double) sizeT;
			}
			
			System.out.println("probS: " + probS);
			System.out.println("probT: " + probT);
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			getParameters(context);
			valueReplicationS = new HashMap<Long,Long>();
			valueReplicationT = new HashMap<Long,Long>();
			super.setup(context);
		}
		
		@Override
		protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			
			Iterator<Entry<Long,Long>> it = valueReplicationS.entrySet().iterator();
			while (it.hasNext()){
				Entry<Long,Long> entry = it.next();
				LongPair lp = new LongPair(entry.getKey(), entry.getValue());
				context.write(keyS, lp);
			}
			
			it = valueReplicationT.entrySet().iterator();
			while (it.hasNext()){
				Entry<Long,Long> entry = it.next();
				LongPair lp = new LongPair(entry.getKey(), entry.getValue());
				context.write(keyT, lp);
			}
			
			super.cleanup(context);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			if (line.trim().equals("")) {
				return;
			}

			String[] strToks = line.split(",");

			if (strToks != null) {					
				double rndm = Math.random();
				
				if (strToks[1].equals("S")) {
					if (rndm <= probS){
						long number = Long.valueOf(strToks[2]);
						long replication = 0;
						
						if (valueReplicationS.containsKey(number)){
							replication = valueReplicationS.get(number);
						}
						
						++replication;
						
						valueReplicationS.put(number, replication);
					}
				}
				else {
					if (rndm <= probT){
						long number = Long.valueOf(strToks[2]);
						long replication = 0;
						
						if (valueReplicationT.containsKey(number)){
							replication = valueReplicationT.get(number);
						}
						
						++replication;
						
						valueReplicationT.put(number, replication);
					}
				}
			}
		}
	}
	
	public static class EDH1CombinerCompressed extends Reducer<IntWritable, LongPair, IntWritable, LongPair> {
		@Override
		protected void reduce(IntWritable key, Iterable<LongPair> values,Context context) throws IOException, InterruptedException {
			
			HashMap<Long,Long> valueReplication = new HashMap<Long,Long>();
			
			Iterator<LongPair> itValues = values.iterator();
			while (itValues.hasNext()) {
				LongPair lp = itValues.next();
				
				long number = lp.getFirst();
				long replication = 0;
				
				if (valueReplication.containsKey(number)){
					replication = valueReplication.get(number);
				}
				
				replication += lp.getSecond();
				
				valueReplication.put(number, replication);
			}
			
			Iterator<Entry<Long,Long>> itHM = valueReplication.entrySet().iterator();
			while (itHM.hasNext()) {
				Entry<Long,Long> entry = itHM.next();
				
				LongPair lp = new LongPair(entry.getKey(), entry.getValue());
				context.write(key,lp);
			}
		}
	}
	
	public static class EDH1ReducerCompressed extends Reducer<IntWritable, LongPair, Text, Text> {
				
		private int k;
		
		private void getParameters(Context context) {
			
			k = Integer.parseInt(context.getConfiguration().get("buckets"));
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			getParameters(context);
			super.setup(context);
		}

		public void reduce(IntWritable key, Iterable<LongPair> values, Context context) throws IOException, InterruptedException {
			
			String relName = (key.get() == 0) ? "S" : "T";
			
			TreeMap<Long,Long> valueReplication = new TreeMap<Long,Long>();
			
			Iterator<LongPair> itValues = values.iterator();
			while (itValues.hasNext()) {
				LongPair ip = itValues.next();
				
				long number = ip.getFirst();
				long replication = 0;
				
				if (valueReplication.containsKey(number)){
					replication = valueReplication.get(number);
				}
				
				replication += ip.getSecond();
				
				valueReplication.put(number, replication);
			}
			
			DecideBoundariesCompressed dbc = new DecideBoundariesCompressed();
			AssignmentResult ar = null;
			try {
				ar = dbc.decideIndexes(valueReplication,k);
			} catch (HistogramsError e) {
				e.printStackTrace();
				String errorMessage = 	"ERROR - EquiDepthHistograms1Compressed#reduce(), error" 	+ 
										" Did not manage to decide the boundaries of the dataset" 	+
										" at reducer with key: " + key.toString();
				throw new InterruptedException(errorMessage);
			}
			
			/*	Boundaries	*/
			long[] kVals = new long[k-1];
			
			Bucket[] buckets = ar.getBuckets();
			for (int i = 0 ; i < k-1 ; ++i) {
				kVals[i] = dbc.getCellValue()[buckets[i].getTo()];
			}
			
			/*	Used for the merging. First and last values.	*/
			context.write(new Text("first_"+relName), new Text(String.valueOf(valueReplication.firstEntry().getKey())));
			context.write(new Text("last_"+relName), new Text(String.valueOf(valueReplication.lastEntry().getKey())));
			
			/*	Writing the boundaries	*/
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < (k-1); ++i) {
				sb.append(Long.toString(kVals[i]));
				if (i+1 < (k-1)) {
					sb.append(",");
				}
			}
			
			context.write(new Text(relName), new Text(sb.toString()));
		}
	}
	
	private void mergeBoundaries() throws IOException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		long[] upperBoundariesS = new long[buckets-1];
		long[] upperBoundariesT = new long[buckets-1];
		
		long firstS = Long.MIN_VALUE;
		long lastS = Long.MIN_VALUE;
		long firstT = Long.MIN_VALUE;
		long lastT = Long.MIN_VALUE;
		
		FileStatus[] boundariesParts = fs.listStatus(outputHistograms1);
		for (int i = 0 ; i < boundariesParts.length; ++i) {
			if (boundariesParts[i].getPath().getName().startsWith("part")) {
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(boundariesParts[i].getPath())));
				String line;
                while ((line=br.readLine()) != null){
            		String[] values = line.split("\t")[1].trim().split(",");
                	if (line.startsWith("S")) {
                		for (int j = 0 ; j < values.length ; ++j) {
                			upperBoundariesS[j] = Integer.parseInt(values[j]);
                		}
                	}
                	else if (line.startsWith("T")){
                		for (int j = 0 ; j < values.length ; ++j) {
                			upperBoundariesT[j] = Integer.parseInt(values[j]);
                		}
                	} else if (line.startsWith("first_S")) {
                		firstS = Integer.valueOf(values[0]);
                	} else if (line.startsWith("last_S")) {
                		lastS = Integer.valueOf(values[0]);
                	} else if (line.startsWith("first_T")) {
                		firstT = Integer.valueOf(values[0]);
                	} else if (line.startsWith("last_T")) {
                		lastT = Integer.valueOf(values[0]);
                	}
                }
                br.close();
			}
		}

		/*	Write boundaries (from,to] to boundaries.csv	*/
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(boundaries,true)));
		
		out.write("S"+","+ (firstS==Long.MIN_VALUE?upperBoundariesS[0]:firstS) + "," + upperBoundariesS[0]);
		out.newLine();
		for (int i = 1 ; i < upperBoundariesS.length; ++i) {
			out.write("S" + "," + (upperBoundariesS[i-1] + 1) + "," + upperBoundariesS[i]);
			out.newLine();
		}
		out.write("S"+","+ (upperBoundariesS[upperBoundariesS.length-1]+1) + "," +
				  (lastS==Long.MIN_VALUE?upperBoundariesS[upperBoundariesS.length-1]:lastS));
		
		out.newLine();
		
		out.write("T"+","+(firstT==Long.MIN_VALUE?upperBoundariesT[0]:firstT) + "," + upperBoundariesT[0]);
		out.newLine();
		for (int i = 1 ; i < upperBoundariesT.length; ++i) {
			out.write("T" + "," + (upperBoundariesT[i-1] + 1) + "," + upperBoundariesT[i]);
			out.newLine();
		}
		out.write("T"+","+ (upperBoundariesT[upperBoundariesT.length-1]+1) + "," + 
				  (lastT==Long.MIN_VALUE?upperBoundariesT[upperBoundariesT.length-1]:lastT));
		
		out.close();
	}
	
	public void run(){
		try
		{
			Configuration conf = new Configuration();
			
			/*	1 hour	*/
			long milliSeconds = 60 * 60 * 1000; // default is 600000, likewise can give any value.
			conf.setLong("mapred.task.timeout", milliSeconds);
			
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outputHistograms1, true);
	
			conf.set("sizeS", String.valueOf(sizeS));
			conf.set("sizeT", String.valueOf(sizeT));
			conf.set("samplingRatio", samplingRatio);
			conf.set("buckets", String.valueOf(buckets));
			
			Job job = new Job(conf, "EquiDepthHistograms1Compressed");
	
			FileInputFormat.addInputPath(job, dataset);
			FileOutputFormat.setOutputPath(job, outputHistograms1);
	
			job.setJarByClass(EquiDepthHistograms1Compressed.class);
	
			job.setMapperClass(EDH1MapperCompressed.class);
			job.setCombinerClass(EDH1CombinerCompressed.class);
			job.setReducerClass(EDH1ReducerCompressed.class);
	
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(LongPair.class);
	
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
				
			long start = System.currentTimeMillis();
			job.waitForCompletion(true);
			long end = System.currentTimeMillis();
			executionTime = end-start;
			
			mergeBoundaries();
		} catch(IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public long getExecutionTime() {
		return executionTime;
	}

}
