package histogram.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

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

/**
 * @deprecated
 * 
 * @author John Koumarelas
 *
 */
public class EquiDepthHistograms1 {
	
	private long sizeS;
	private long sizeT;
	private String samplingRatio;
	private int buckets;
	private Path dataset;
	private Path boundaries;
	
	private final static Path outputHistograms1 = new Path("btj/histograms1");
	
	private long executionTime;
	
	public long getExecutionTime() {
		return executionTime;
	}

	public EquiDepthHistograms1(long sizeS, long sizeT, String samplingRatio, int buckets, Path dataset, Path boundaries) {
		this.sizeS = sizeS;
		this.sizeT = sizeT;
		this.samplingRatio = samplingRatio;
		this.buckets = buckets;
		this.dataset = dataset;
		this.boundaries = boundaries;
	}

	public static class EDH1Mapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		private long sizeS; 
		private long sizeT;
		private long n;
		private double probS;
		private double probT;
		
		private static final IntWritable keyS = new IntWritable(0);
		private static final IntWritable keyT = new IntWritable(1);
		
		private void getParameters(Context context) {
			sizeS = Long.parseLong(context.getConfiguration().get("sizeS"));
			sizeT = Long.parseLong(context.getConfiguration().get("sizeT"));
			
			String strN = context.getConfiguration().get("samplingRatio");
			if(strN.endsWith("%")) {
				strN = strN.substring(0, strN.indexOf("%"));
				n = Long.parseLong(strN);
				probS = n / 100.0;
				probT = n / 100.0;
			} else {
				n = Long.parseLong(context.getConfiguration().get("samplingRatio"));
				probS = n / (double) sizeS;
				probT = n / (double) sizeT;
			}
			
			System.out.println("probS: " + probS);
			System.out.println("probT: " + probT);
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			getParameters(context);
			super.setup(context);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			if(line.trim().equals("")){
				return;
			}

			String[] strToks = line.split(",");

			if(strToks != null) {					
					double rndm = Math.random();
					
					if(strToks[0].equals("S")) {
						if(rndm <= probS){
							context.write(keyS, value);
						}
					}
					else {
						if(rndm <= probT){
							context.write(keyT, value);
						}
					}
			}
		}
	}
	
	public static class EDH1Combiner extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
					Context context)
				throws IOException, InterruptedException {
			HashSet<String> hs = new HashSet<String>();
			
			Iterator<Text> itVal = values.iterator();
			while(itVal.hasNext())
				hs.add(itVal.next().toString());
			
			Iterator<String> itHs = hs.iterator();
			while(itHs.hasNext())
				context.write(key, new Text(itHs.next()));
		}
	}
	
	public static class EDH1Reducer extends
			Reducer<IntWritable, Text, Text, Text> {
		
		private LinkedList<Long> tuples = new LinkedList<Long>();
		
		private int k;
		
		private void getParameters(Context context) {
			k = Integer.parseInt(context.getConfiguration().get("buckets"));
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			getParameters(context);
			super.setup(context);
		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			tuples.clear();
			
			String relName;
			if(key.get() == 0){
				relName = "S";
			}
			else {
				relName = "T";
			}
			
			Iterator<Text> valuesIt = values.iterator();
			while(valuesIt.hasNext()) {
				String tuple = new String(valuesIt.next().toString()); 
				
				if(tuple.trim().equals("")) {
					continue;
				}
				
				String[] strToks = tuple.split(",");
				
				tuples.add(Long.parseLong(strToks[1]));
			}
			
			if(tuples.size() < k) {
				System.out.println("\n!!! Tuples arrived at histogram1.reducers less than k !!! Aborting !!!");
				System.exit(-1);
			}
			
			Collections.sort(tuples);
			
			
			long[] kVals = new long[k];
			if(k == tuples.size()) {
//				for(int i = 0 ; i < k-1 ; ++i)
//					kVals[i] = tuples.get(i);
//				kVals[k-1] = Integer.MAX_VALUE;
				for(int i = 0 ; i < k ; ++i) {
					kVals[i] = tuples.get(i);
				}
			}
			else {
				int counter = 0;
				
//				kVals[0] = tuples.getFirst();
				int step = (int) (tuples.size() / (double) k);
				int i = step;
				while(counter<=(k-2)) {
					
					// Case where the following value is the same as before
 					while((counter>0) && ((i)<(tuples.size()-1)) && kVals[counter-1] == tuples.get(i)) {
						++i;
						// XXX: danger of going outside of boundaries
					}
						
					kVals[counter++] = tuples.get(i);
					i+= step;
				}
				kVals[k-1] = kVals[k-2]+1;
//				kVals[k-1] = tuples.getLast();
				
				context.write(new Text("first_"+relName), new Text(String.valueOf(tuples.getFirst())));
				context.write(new Text("last_"+relName), new Text(String.valueOf(tuples.getLast())));
			}
			
			for(int i = 0 ; i < k-1 ; ++i) {
				if(kVals[i] == kVals[i+1]) {
					System.out.println("\n!!! Warning: Same boundaries !!! Aborting !!!");
//					break;
					System.exit(-1);
				}
			}
			
			StringBuffer sb = new StringBuffer();

			for(int i = 0; i < k; ++i) {
				sb.append(Long.toString(kVals[i]));
				if(i+1 < k) {
					sb.append(",");
				}
			}
			
//			sb.append(" | tuples arrived: " + tuples.size());
//			sb.append(" step: " + step);
//			sb.append(" tuples: ");
//			for(int i = 0 ; i < tuples.size(); ++i) {
//				sb.append(tuples.get(i) + ",");
//			}
			
			context.write(new Text(relName), new Text(sb.toString()));
			
//			context.write(new Text("S"), new Text(sb.toString()));
//			context.write(new Text("T"), new Text(sb.toString()));
		}
	}
	
	private void mergeBoundaries() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		
		long[] upperBoundariesS = new long[buckets];
		long[] upperBoundariesT = new long[buckets];
		
		long firstS = Long.MIN_VALUE;
		long lastS = Long.MIN_VALUE;
		long firstT = Long.MIN_VALUE;
		long lastT = Long.MIN_VALUE;
		
		FileStatus[] boundariesParts = fs.listStatus(outputHistograms1);
		for(int i = 0 ; i < boundariesParts.length; ++i) {
			if(boundariesParts[i].getPath().getName().startsWith("part")) {
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(boundariesParts[i].getPath())));
				String line;
                while ((line=br.readLine()) != null){
            		String[] values = line.split("\t")[1].trim().split(",");
                	if(line.startsWith("S")) {
                		for(int j = 0 ; j < values.length ; ++j) {
                			upperBoundariesS[j] = Long.parseLong(values[j]);
                		}
                	}
                	else if(line.startsWith("T")){
                		for(int j = 0 ; j < values.length ; ++j) {
                			upperBoundariesT[j] = Long.parseLong(values[j]);
                		}
                	} else if(line.startsWith("first_S")) {
                		firstS = Long.valueOf(values[0]);
                	} else if(line.startsWith("last_S")) {
                		lastS = Long.valueOf(values[0]);
                	} else if(line.startsWith("first_T")) {
                		firstT = Long.valueOf(values[0]);
                	} else if(line.startsWith("last_T")) {
                		lastT = Long.valueOf(values[0]);
                	}
                }
                br.close();
			}
		}

		// Write boundaries (from,to] to boundaries.csv
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				fs.create(boundaries,true)));
		
		out.write("S"+","+ (firstS==Long.MIN_VALUE?upperBoundariesS[0]:firstS) + "," + upperBoundariesS[0]);
		out.newLine();
		for(int i = 1 ; i < upperBoundariesS.length-1; ++i) {
			out.write("S" + "," + (upperBoundariesS[i-1] + 1) + "," + upperBoundariesS[i]);
			if((i+1)<upperBoundariesS.length){
				out.newLine();
			}
		}
		out.write("S"+","+ upperBoundariesS[upperBoundariesS.length-1]+ "," + (lastS==Long.MIN_VALUE?upperBoundariesS[upperBoundariesS.length-1]:lastS));
		
		out.newLine();
		
		out.write("T"+","+(firstT==Long.MIN_VALUE?upperBoundariesT[0]:firstT) + "," + upperBoundariesT[0]);
		out.newLine();
		for(int i = 1 ; i < upperBoundariesT.length-1; ++i) {
			out.write("T" + "," + (upperBoundariesT[i-1] + 1) + "," + upperBoundariesT[i]);
			if((i+1)<upperBoundariesT.length){
				out.newLine();
			}
		}
		out.write("T"+","+ upperBoundariesT[upperBoundariesT.length-1]+ "," + (lastT==Long.MIN_VALUE?upperBoundariesT[upperBoundariesT.length-1]:lastT));
		
//		out.write("S"+","+Integer.MIN_VALUE + "," + upperBoundariesS[0]);
//		out.newLine();
//		for(int i = 1 ; i < upperBoundariesS.length; ++i) {
//			out.write("S" + "," + (upperBoundariesS[i-1] + 1) + "," + upperBoundariesS[i]);
//			if((i+1)<upperBoundariesS.length){
//				out.newLine();
//			}
//		}
//		
//		out.newLine();
//		
//		out.write("T"+","+Integer.MIN_VALUE + "," + upperBoundariesT[0]);
//		out.newLine();
//		for(int i = 1 ; i < upperBoundariesT.length; ++i) {
//			out.write("T" + "," + (upperBoundariesT[i-1] + 1) + "," + upperBoundariesT[i]);
//			if((i+1)<upperBoundariesT.length){
//				out.newLine();
//			}
//		}
		
		out.close();
	}
	
	public void run(){
		try
		{
//			Monitoring.printMessage("msg.mbi.hist.1.start");
			Configuration conf = new Configuration();
			
			// 1 hour
			long milliSeconds = 60 * 60 * 1000; // <default is 600000, likewise can give any value)
			conf.setLong("mapred.task.timeout", milliSeconds);
			
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outputHistograms1, true);
	
			conf.set("sizeS", String.valueOf(sizeS));
			conf.set("sizeT", String.valueOf(sizeT));
			conf.set("samplingRatio", samplingRatio);
			conf.set("buckets", String.valueOf(buckets));
			
			Job job = new Job(conf, "EquiDepthHistograms1");
	
			FileInputFormat.addInputPath(job, dataset);
			FileOutputFormat.setOutputPath(job, outputHistograms1);
	
			job.setJarByClass(EquiDepthHistograms1.class);
	
			job.setMapperClass(EDH1Mapper.class);
			job.setCombinerClass(EDH1Combiner.class);
			job.setReducerClass(EDH1Reducer.class);
	
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
	
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
				
			long start = System.currentTimeMillis();
			job.waitForCompletion(true);
			long end = System.currentTimeMillis();
			executionTime = end-start;
			
			mergeBoundaries();
					
//			Monitoring.printMessage("msg.mbi.hist.1.end");
		}
		catch(IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
