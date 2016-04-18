/**
 * EquiDepthHistograms2.java
 * 
 * This class is responsible for counting the number of values that fall into each bucket,
 * namely the counter of the bucket. It loads the boundaries from the previous phase, which
 * calculated these boundaries.
 * 
 * Results in: counts.csv
 * 
 * @author John Koumarelas
 */

package histogram.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import model.BucketBoundaries;

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

import utils.importers.HistogramImporter;
import datatypes.IntPair;

public class EquiDepthHistograms2 {
	private int buckets;
	private Path dataset;
	
	private static int numReducers = 1;
	
	private Path boundaries;
	private Path counts;
	
	private final static Path outputHistograms2 = new Path("btj/histograms2");
	
	private long executionTime;
	
	public long getExecutionTime() {
		return executionTime;
	}

	public EquiDepthHistograms2( int buckets, Path dataset, Path boundaries, Path counts) {
		this.buckets = buckets;
		this.dataset = dataset;
		this.boundaries = boundaries;
		this.counts = counts;
	}
	
	public static class EDH2Mapper extends
			Mapper<LongWritable, Text, IntPair, LongWritable> {
		
		private BucketBoundaries[] boundariesS;
		private BucketBoundaries[] boundariesT;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			int buckets = Integer.valueOf(conf.get("buckets"));
			Path boundaries = new Path(conf.get("boundaries"));
						
			
			boundariesS = new HistogramImporter().importBoundaries("S", buckets, boundaries);
			boundariesT = new HistogramImporter().importBoundaries("T", buckets, boundaries);
		}
		
		public int matrixToReducerMapping(long value, String tupleOrigin)
		{
			BucketBoundaries[] boundaries = (tupleOrigin.equals("S")?boundariesS:boundariesT);
			
			// TODO: replace with binary search
			for(int i = 0; i < boundaries.length ; ++i) {
				if(value>= boundaries[i].getFrom() && value <= boundaries[i].getTo()){
					return i;
				}
			}
			
			return -1;
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			if(line.trim().equals("")) {
				return;
			}
			
			String[] strToks = line.split(",");

			if(strToks != null) {
				if(strToks.length >= 2)
				{
					int histogramsIndex = matrixToReducerMapping(Long.parseLong(strToks[2]), strToks[1]);
					IntPair ip = new IntPair(strToks[1].equals("S")?0:1, histogramsIndex);
					context.write(ip, new LongWritable(1L));
				}
			}
		}
	}
	
	public static class EDH2Reducer extends
			Reducer<IntPair, LongWritable, IntPair, LongWritable> {
		
		public void reduce(IntPair key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			Iterator<LongWritable> itValues = values.iterator();
			long sum = 0;
			while(itValues.hasNext()) {
				sum += itValues.next().get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	
	private void mergeCounts() throws IOException {
		long[] countsS = new long[buckets];
		long[] countsT = new long[buckets];
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		FileStatus[] countsParts = fs.listStatus(outputHistograms2);
		for(int i = 0 ; i < countsParts.length; ++i) {
			if(countsParts[i].getPath().getName().startsWith("part")) {
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(countsParts[i].getPath())));
				String line;
                while ((line=br.readLine()) != null){
                	String[] countsTokens = line.split("\t");
            		String[] countsIndex = countsTokens[0].substring(countsTokens[0].indexOf('[') + 1, countsTokens[0].indexOf(']')).split(",");
                	
            		long[] counts = (countsIndex[0].equals("0")?countsS:countsT);
            		counts[Integer.parseInt(countsIndex[1])] = Long.parseLong(countsTokens[1]);
                }
                br.close();
			}
		}
		
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				fs.create(counts,true)));
						
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
	}
	
	public void run() {
		try
		{
//			Monitoring.printMessage("msg.mbi.hist.2.start");			
			Configuration conf = new Configuration();
			
			// 1 hour
			long milliSeconds = 60 * 60 * 1000; // <default is 600000, likewise can give any value)
			conf.setLong("mapred.task.timeout", milliSeconds);
			
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outputHistograms2, true);
			
			conf.set("boundaries", boundaries.toString());
			conf.set("buckets", String.valueOf(buckets));
			
			Job job = new Job(conf, "EquiDepthHistograms2");
	
			FileInputFormat.addInputPath(job, dataset);
			FileOutputFormat.setOutputPath(job, outputHistograms2);
	
			job.setJarByClass(EquiDepthHistograms2.class);
	
			job.setMapperClass(EDH2Mapper.class);
			job.setCombinerClass(EDH2Reducer.class);
			job.setReducerClass(EDH2Reducer.class);
	
			job.setMapOutputKeyClass(IntPair.class);
			job.setMapOutputValueClass(LongWritable.class);
	
			job.setOutputKeyClass(IntPair.class);
			job.setOutputValueClass(LongWritable.class);
			
			// XXX
			job.setNumReduceTasks(numReducers);
			
			long start = System.currentTimeMillis();
			job.waitForCompletion(true);
			long end = System.currentTimeMillis();
			
			executionTime = end-start;
			
			mergeCounts();
		
//			Monitoring.printMessage("msg.mbi.hist.2.end");
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
